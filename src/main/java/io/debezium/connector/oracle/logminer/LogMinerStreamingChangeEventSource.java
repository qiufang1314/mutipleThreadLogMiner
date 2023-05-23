/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.oracle.logminer;

import com.alibaba.fastjson.JSON;
import com.asw.mdm.stream.etl.config.Configurations;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.*;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.*;
import io.debezium.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.errors.ConnectException;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.*;

/**
 * 如果 current scn 值远远大于 startScn，就触发多个connection 解析log，只解析 archive log
 * 简单些做，先解析，收到数据放到内存，全部解析完成了，再按序遍历事件
 * 优化下：
 * eg:
 * start scn, end scn(1-550), batchSize: 每一个logminer 线程解析的范围(100)， parallelNum 并发logminer解析度(6)
 * 生成spit(startScn,endScn), (start,endScn]前开后闭
 * spit-1（1,100]
 * spit-2（100,200]
 * spit-3（200,300]
 * spit-4（300,400]
 * spit-5（400,500]
 * spit-6（500,550]
 *
 * parallelNum-{1-6}  去拿split
 * parallelNum-2 获取到，spit-1，logminer 解析，因为是最前面批次，可以边解析边处理
 * parallelNum-5 获取到，spit-2，logminer 解析，不是首批次，需要将解析的数据全部放到内存，
 *
 * parallelNum-5解析完成后，怎么获取下spit？ 解析只管解析，不应该与spit合在一起考虑， 那就需要一个 assign spit线程
 *
 * assign spit线程，startScn=preEndScn, endScn=next range
 *
 * process thread 去获取解析完的数据处理，需要按序那一批数据，根据startScn去查找，没查到表示这批数据还没有解析完成，需要等待，
 *
 * 做的有点点不一样，后续再补文档~~
 *
 */
// TODO Garry: start scn, commite offset scn, end scn, save point,  interrupted exception
@Slf4j
public class LogMinerStreamingChangeEventSource
        implements StreamingChangeEventSource<OracleOffsetContext> {

    /**
     * 顺序获取大日志保存的 批次，防止内存溢出
     */
    public static final int SEQ_LOG_PARALLEL = 20;

    /**
     * scn range 派发的最大个数
     */
    public static final int SCN_SPLIT_CAPACITY = SEQ_LOG_PARALLEL * 3;

    /**
     * scn range 大小，值也会根据当前大scn值做判断
     */
    public static final int SCN_BATCH_RANGE = 3000;

    /**
     *log miner 获取并发度
     */
    public static final int LOG_FETCHER_PARALLEL = 3;

    /**
     * 计算的end scn 小于 current scn的个数
     */
    public static final int END_SCN_LT_CURRENT_SCN_NUM = 3;

    // parallel logminer thread
    private int parallelNum = 5;
    private OracleConnection jdbcConnection;
    private  EventDispatcher<TableId> dispatcher;
    private  Clock clock;
    private  OracleDatabaseSchema schema;
    private  boolean isRac;
    private  Set<String> racHosts = new HashSet<>();
    private  JdbcConfiguration jdbcConfiguration;
    private  OracleConnectorConfig.LogMiningStrategy strategy;
    private  ErrorHandler errorHandler;
    private  boolean isContinuousMining;
    private  OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private  OracleConnectorConfig connectorConfig;
    private  Duration archiveLogRetention;
    private  boolean archiveLogOnlyMode;
    private  String archiveDestinationName;

    private Scn startScn;
    private Scn endScn;

    Configuration jdbcConfig;

    ScnSplitAssign scnSplitAssign;
    SplitLogFetcherManager splitLogFetcherManager;
    DynamicAdjustLogFetcherManager dynamicAdjustLogFetcherManager;
    /**
     * 顺序获取log event
     */
    SeqLogFetchManager seqLogFetchManager;
    LogProcessorContext logProcessorContext;

    LogEventBatch lastLogEventBatch;
    LogEventBatch currentLogEventBatch;

    public LogMinerStreamingChangeEventSource(
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            Configuration jdbcConfig,
            OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.jdbcConfig = jdbcConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.isRac = connectorConfig.isRacSystem();
        if (this.isRac) {
            this.racHosts.addAll(
                    connectorConfig.getRacNodes().stream()
                            .map(String::toUpperCase)
                            .collect(Collectors.toSet()));
            instantiateFlushConnections(jdbcConfiguration, racHosts);
        }
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
        this.archiveLogOnlyMode = connectorConfig.isArchiveLogOnlyMode();
        this.archiveDestinationName = connectorConfig.getLogMiningArchiveDestinationName();

    }

    @Override
    public void init() throws InterruptedException {
        StreamingChangeEventSource.super.init();
    }

    @Override
    public void execute(ChangeEventSourceContext context, OracleOffsetContext offsetContext) throws InterruptedException {

        TransactionalBuffer transactionalBuffer =
                new TransactionalBuffer(
                        connectorConfig, schema, clock, errorHandler, streamingMetrics);

        logProcessorContext = new LogProcessorContext();
//        logProcessorContext.setScnSplitQueueCapacity(SCN_SPLIT_CAPACITY);
//        logProcessorContext.setSeqLogParallel(SEQ_LOG_PARALLEL);
//        logProcessorContext.setScnBatchRange(SCN_BATCH_RANGE);
//        logProcessorContext.setLogFetchParallel(LOG_FETCHER_PARALLEL);
        logProcessorContext.setEndScnLtCurrentScnNum(END_SCN_LT_CURRENT_SCN_NUM);


        Configuration config = connectorConfig.getConfig();

        logProcessorContext.setScnSplitQueueCapacity(config.getInteger("scnSplitQueueCapacity"));
        logProcessorContext.setPendingLogMinerQueueCapacity(config.getInteger("pendingLogMinerQueueCapacity"));
        logProcessorContext.setSeqLogQueueCapacity(config.getInteger("seqLogQueueCapacity"));
        logProcessorContext.setScnBatchRange(config.getInteger("scnBatchRange"));
        logProcessorContext.setLogFetchParallel(config.getInteger("logFetchParallel"));
        logProcessorContext.setLogPollBatchSize(config.getInteger("logPollBatchSize"));
        logProcessorContext.setSplitLogFetchResultQueueSize(config.getInteger("splitFetchQueueSize"));
        logProcessorContext.setMaxLogFetcherThread(config.getInteger("maxLogFetcherThread"));

        log.info("logProcessorContext value:{}", JSON.toJSON(logProcessorContext));

        logProcessorContext.setContext(context);

        BlockingQueue splitQueue = new ArrayBlockingQueue(logProcessorContext.getScnSplitQueueCapacity());
        BlockingQueue<ScnSplitInfo> pendingLogMinerQueue = new ArrayBlockingQueue<>(logProcessorContext.getPendingLogMinerQueueCapacity());

        try {
            startScn = offsetContext.getScn();
            // check if scn in logfile to avoid restart fail
//            boolean scnInlogfile = LogMinerRepository.isScnInlogfile(jdbcConnection, startScn,isContinuousMining);
            // 不同Oracle版本使用的sql有些不同，后面再兼容高版本
            if (isContinuousMining){
                Scn earliestSCN = searchEarliestSCN();
                if (startScn.compareTo(earliestSCN) < 0) {
                    createRestoreDataEvents(context,offsetContext,startScn.toString(),earliestSCN.toString());
                    startScn = earliestSCN;
                }
            }

            if (!isContinuousMining
                    && startScn.compareTo(
                    getFirstOnlineLogScn(
                            jdbcConnection,
                            archiveLogRetention,
                            archiveDestinationName))
                    < 0) {
                Scn firstOnlineLogScn = getFirstOnlineLogScn(
                        jdbcConnection,
                        archiveLogRetention,
                        archiveDestinationName);
                log.info("the first online log in Oracle is {},and startScn is {} " ,firstOnlineLogScn,startScn);
                throw new DebeziumException(
                        "Online REDO LOG files or archive log files do not contain the offset scn "
                                + startScn
                                + ".  Please perform a new snapshot.");
            }

            setNlsSessionParameters(jdbcConnection);
            checkSupplementalLogging(jdbcConnection, connectorConfig.getPdbName(), schema);

            // 切换scn range
            scnSplitAssign = new ScnSplitAssign(logProcessorContext, jdbcConnection, splitQueue, startScn);

            // 顺序获取解析后的log数据
            splitLogFetcherManager = new SplitLogFetcherManager(pendingLogMinerQueue,
                    logProcessorContext,
                    connectorConfig,
                    jdbcConnection,
                    errorHandler,
                    schema,
                    jdbcConfig,
                    streamingMetrics);
            seqLogFetchManager = new SeqLogFetchManager(splitQueue, pendingLogMinerQueue, logProcessorContext);
            dynamicAdjustLogFetcherManager = new DynamicAdjustLogFetcherManager(logProcessorContext,splitLogFetcherManager, jdbcConnection);

            startComponent();

            process(context, offsetContext, transactionalBuffer,jdbcConnection);

        } catch (Throwable t) {
            logError(streamingMetrics, "Mining session stopped due to the {}", t);
            errorHandler.setProducerThrowable(t);
        } finally {

            stopComponent();
            log.info(
                    "startScn={}, endScn={}, offsetContext.getScn()={}",
                    startScn,
                    endScn,
                    offsetContext.getScn());
            log.info("Transactional buffer dump: {}", transactionalBuffer.toString());
            log.info("Streaming metrics dump: {}", streamingMetrics.toString());
        }

    }

    private void process(ChangeEventSourceContext context, OracleOffsetContext offsetContext,
                         TransactionalBuffer transactionalBuffer,OracleConnection jdbcConnection) throws InterruptedException, SQLException {
        LogEventProcessor logEventProcessor = new LogEventProcessor(
                logProcessorContext,
                context,
                connectorConfig,
                streamingMetrics,
                transactionalBuffer,
                offsetContext,
                schema,
                dispatcher,jdbcConnection);

        Stopwatch accumulatorSt = Stopwatch.accumulating().start();

        while (context.isRunning()) {
            // log fetch 异常
            if (splitLogFetcherManager.unRecoverableException()) {
                stopComponent();
                throw new RuntimeException(splitLogFetcherManager.getLogMinerError());
            }
            if (currentLogEventBatch != null) {
                lastLogEventBatch = currentLogEventBatch;
            }

            // 有log数据，或者等待超时，或者这个分片已经执行完成
            currentLogEventBatch = seqLogFetchManager.awaitToGetNextBatchLog();

            boolean splitScnFetchFinished = currentLogEventBatch.isSplitScnFetchFinished();
            if (splitScnFetchFinished || !currentLogEventBatch.isEmpty()) {
                checkLogSequences(lastLogEventBatch, currentLogEventBatch);

                accumulatorSt.stop();
                boolean needPrintLog = needToPrintLogInfo(currentLogEventBatch, accumulatorSt);
                if (needPrintLog) {
                    // reset
                    accumulatorSt = Stopwatch.accumulating();
                }
                accumulatorSt.start();

                logEventProcessor.processResult(currentLogEventBatch, needPrintLog);
                afterHandleScn(offsetContext);
            }

        }
    }

    private boolean needToPrintLogInfo(LogEventBatch currentLogEventBatch, Stopwatch accumulatorSt) {

        boolean splitScnFetchFinished = currentLogEventBatch.isSplitScnFetchFinished();
        if (splitScnFetchFinished) {
            return true;
        }

        Duration totalDuration = accumulatorSt.durations().statistics().getTotal();
        if (totalDuration.compareTo(Duration.ofMinutes(1)) >= 0) {
            return true;
        }

        return false;

    }

    //oracle删除redolog中的日志不一定是从前往后删除，可能是在中间任意一个文件中删除
    //1.因此要将log,archivelog中所有的scn进行一次判断，找到最旧的能用的scn且这个scn之后的日志文件必须保证都能找到
    //2.在自动恢复时，可能offsetSCN是在logfile中的，但是之后的scn可能会中断
    private Scn searchEarliestSCN() throws SQLException {
        Scn restartScn = null;
        long retetionHours = connectorConfig.getLogMiningTransactionRetention().toHours();
        if (retetionHours==0){
            retetionHours=4;
        }
        ArrayList<String> archiveLogList = LogMinerRepository.getArchiveLogList(jdbcConnection, retetionHours);
        ArrayList<String> logList = LogMinerRepository.getLogList(jdbcConnection);
        ArrayList<AvailableScnVO> aScn = new ArrayList<>();
        archiveLogList.addAll(logList);
        archiveLogList.forEach(scnStr -> {
            Scn scn = new Scn(new BigInteger(scnStr));
            boolean scnInlogfile = false;
            try {
                scnInlogfile = LogMinerRepository.isScnInlogfile(jdbcConnection, scn, isContinuousMining);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            log.info("AvailableScnVO scn = {}, scnInlogfile = {}", scn, scnInlogfile);
            aScn.add(new AvailableScnVO(scn, scnInlogfile));
        });

        for (int i = aScn.size() - 1; i > 0; i--) {
            AvailableScnVO availableScnVO = aScn.get(i);
            if (!availableScnVO.isScnInlogfile()) {
                log.info("use AvailableScnVO ={}", restartScn);
                break;
            }
            restartScn = availableScnVO.getScn();
        }
        if (restartScn != null) {
//            log.error("Start SCN is not in logfile ,will start with restartScn={} , pls check job log carefully", restartScn);

            return restartScn;
        } else {
            String currentScn = LogMinerRepository.getCurrentScn(jdbcConnection);
//            log.error("Start SCN is not in logfile ,will start with currentSCN={} , pls check job log carefully", currentScn);
            return new Scn(new BigInteger(currentScn));
        }

    }

    //恢复[failSCN,ealiestSCN]的数据
    private void createRestoreDataEvents(ChangeEventSourceContext context, OracleOffsetContext offsetContext, String restoreScn, String currentScn) throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

        final int tableCount = this.schema.getTables().size();

        int tableOrder = 1;
        log.info("recover missing scn [{},{}]", restoreScn,currentScn);
        for (Iterator<TableId> tableIdIterator = this.schema.getTables().tableIds().iterator(); tableIdIterator.hasNext(); ) {
            final TableId tableId = tableIdIterator.next();
//            snapshotContext.lastTable = !tableIdIterator.hasNext();
            Column last_update_datetime = this.schema.getTables().forTable(tableId).columnWithName("LAST_UPDATE_DATETIME");

            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            log.debug("Snapshotting table {}", tableId);

            createRestoreDataEventsForTable(context, offsetContext, snapshotReceiver, this.schema.getTables().forTable(tableId),
                    tableOrder++, tableCount, restoreScn, last_update_datetime, currentScn);
        }


    }

    private void createRestoreDataEventsForTable(ChangeEventSourceContext sourceContext, OracleOffsetContext snapshotContext,
                                                 EventDispatcher.SnapshotReceiver snapshotReceiver, Table table, int tableOrder, int tableCount, String restoreScn, Column lastUpdate, String currentScn)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        log.info("\t Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        final Optional<String> selectStatement = getRestoreSelect(snapshotContext, table.id(), restoreScn, lastUpdate != null, currentScn);
        if (!selectStatement.isPresent()) {
            log.warn("For table '{}' the select statement was not provided, skipping table", table.id());
//            snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), 0);
            return;
        }
        log.info("\t For table '{}' using select statement: '{}'", table.id(), selectStatement.get());
        final OptionalLong rowCount = OptionalLong.empty();
        try (Statement statement = jdbcConnection.readTableStatement(connectorConfig, rowCount);
             ResultSet rs = statement.executeQuery(selectStatement.get())) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = Threads.timer(clock, Duration.ofMillis(10_000));
            boolean lastRecord = false;
            if (rs.next()) {
                while (!lastRecord) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                    }
                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, this.schema, rs, columnArray);
                    lastRecord = !rs.next();
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        if (rowCount.isPresent()) {
                            log.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                    table.id(), Strings.duration(stop - exportStart));
                        } else {
                            log.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                    Strings.duration(stop - exportStart));
                        }
                        logTimer = Threads.timer(clock, Duration.ofMillis(10_000));
                    }

                    dispatcher.dispatchSnapshotEvent(table.id(), getChangeRecordEmitter(snapshotContext, table.id(), row), snapshotReceiver);
                    // 最后一条数据无论如何都需要发两次才能发送成功，暂时不清楚为什么，先这样写着吧
                    if (lastRecord){
                        dispatcher.dispatchSnapshotEvent(table.id(), getChangeRecordEmitter(snapshotContext, table.id(), row), snapshotReceiver);
                    }
                }
            }

            log.info("\t Finished exporting {} records for table '{}'; total duration '{}'", rows,
                    table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
//            snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), rows);
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    protected Optional<String> getRestoreSelect(OracleOffsetContext snapshotContext, TableId tableId, String restoreScn, Boolean hasLastUpdate, String currentScn) {
//        final OracleOffsetContext offset = snapshotContext.offset;
        snapshotContext.setScn(new Scn(new BigInteger(currentScn)));
//        offset.setScn(new Scn(new BigInteger(currentScn)));

        if (hasLastUpdate) {
            return Optional.of("SELECT * FROM " + quote(tableId) +
                    " WHERE cast(LAST_UPDATE_DATETIME AS timestamp) >=(SELECT SCN_TO_TIMESTAMP( " + restoreScn + ") FROM dual)" +
                    "   and cast(LAST_UPDATE_DATETIME AS timestamp) <(SELECT SCN_TO_TIMESTAMP( " + currentScn + ") FROM dual)");
        } else {
            return Optional.of("SELECT * FROM " + quote(tableId) + " WHERE ORA_ROWSCN>= " + restoreScn + " and ORA_ROWSCN< " + currentScn);
        }
//
    }

    private static String quote(TableId tableId) {
        return TableId.parse(tableId.schema() + "." + tableId.table(), true).toDoubleQuotedString();
    }
    protected ChangeRecordEmitter getChangeRecordEmitter(OracleOffsetContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext, row, clock);
    }

    private void checkLogSequences(LogEventBatch lastLogEventBatch, LogEventBatch currentLogEventBatch) {
        if (lastLogEventBatch != null && currentLogEventBatch != null) {
            List<LogEvent> lastLogEvents = lastLogEventBatch.getLogEvents();
            List<LogEvent> currentLogEventBatchLogEvents = currentLogEventBatch.getLogEvents();

            // same split batch
            if (lastLogEventBatch.getStartScn().compareTo(currentLogEventBatch.getStartScn()) == 0) {
                // 事件中 scn 值不是递增
                // current batch first log event scn >= last batch last log event scn
                /*if (CollectionUtils.isNotEmpty(lastLogEvents) && CollectionUtils.isNotEmpty(currentLogEventBatchLogEvents)) {
                    LogEvent logEvent1 = lastLogEvents.get(lastLogEvents.size() - 1);
                    LogEvent logEvent2 = currentLogEventBatchLogEvents.get(0);
                    if (logEvent2.getScn().compareTo(logEvent1.getScn()) < 0) {
                        log.error("last batch last log event scn > current batch first log event, wrong sequence. lastLogEventBatch:{}, currentLogEventBatch:{}",
                                lastLogEventBatch, currentLogEventBatch);
                        throw new IllegalStateException("last batch last log event scn > current batch first log event, wrong sequence. lastLogEventBatch:" +
                                lastLogEventBatch +
                                ", currentLogEventBatch:" +
                                currentLogEventBatch
                        );
                    }
                }*/
            } else if (CollectionUtils.isNotEmpty(lastLogEvents)) {
                // not same batch
                // last batch end log event scn <= current batch start scn
                LogEvent lastLogEvent = lastLogEvents.get(lastLogEvents.size() - 1);
                Scn lastLogScn = lastLogEvent.getScn();
                // last processed scn < current start scn
                Scn currentLogStartScn = currentLogEventBatch.getStartScn();
                if (lastLogScn.compareTo(currentLogStartScn) > 0) {
                    log.error("last batch end log event scn{} > current batch start scn{}, wrong sequence. lastLogEventBatch:{}, currentLogEventBatch:{}",
                            lastLogScn, currentLogStartScn, lastLogEventBatch, currentLogEventBatch);
                    throw new IllegalStateException("last batch end log event scn > current batch start scn, wrong sequence. lastLogEventBatch:" +
                            lastLogEventBatch +
                            ", currentLogEventBatch:" +
                            currentLogEventBatch
                    );
                }
            }

        }
    }

    private void startComponent() {
        //需要先开启
        logProcessorContext.start();

        dynamicAdjustLogFetcherManager.start();
        scnSplitAssign.start();
        splitLogFetcherManager.start();
        seqLogFetchManager.start();
    }

    private void stopComponent() {
        logProcessorContext.stop();

        dynamicAdjustLogFetcherManager.stop();
        scnSplitAssign.stop();
        splitLogFetcherManager.stop();
        seqLogFetchManager.stop();

        try {
            jdbcConnection.close();
        } catch (SQLException e) {
            log.error("close jdbc connection error.", e);
        }

    }

    protected void afterHandleScn(OracleOffsetContext offsetContext) {
    }
}
