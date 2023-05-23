package io.debezium.connector.oracle.logminer;

import com.asw.mdm.stream.etl.config.Configurations;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.*;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.*;

@Slf4j
public class SplitLogFetch implements Runnable {

    public static final int LOGMINER_RETEYABLE_NUM = 3;
    private BlockingQueue<ScnSplitInfo> pendingLogMinerQueue;

    private Duration period = Duration.ofSeconds(5);

    LogProcessorContext logProcessorContext;

    private Set<String> tableIncludeSet = new HashSet<>();

    private String logPrefix;

    private volatile boolean isRunning;

    private volatile Throwable logMinerException;

    Method isRetriableMethod;

    public SplitLogFetch(BlockingQueue<ScnSplitInfo> pendingLogMinerQueue, LogProcessorContext logProcessorContext,
                         OracleConnectorConfig connectorConfig,
                         OracleConnection jdbcConnection,
                         ErrorHandler errorHandler,
                         OracleDatabaseSchema schema,
                         Configuration jdbcConfig,
                         OracleStreamingChangeEventSourceMetrics streamingMetrics,
                         String logPrefix) {
        this.isRunning = true;
        this.logPrefix = logPrefix;
        this.pendingLogMinerQueue = pendingLogMinerQueue;
        this.logProcessorContext = logProcessorContext;
        this.isContinuousMining = connectorConfig.isContinuousMining();

        this.jdbcConnection = jdbcConnection;
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

        String tableIncludeList = connectorConfig.tableIncludeList();
        if (StringUtils.isNotBlank(tableIncludeList)) {
            String[] tableIncludeArray = StringUtils.split(tableIncludeList, ",");
            if (tableIncludeArray != null && tableIncludeArray.length > 0) {
                tableIncludeSet = Arrays.stream(tableIncludeArray).map(o -> StringUtils.lowerCase(o)).collect(Collectors.toSet());
            }
        }

        try {
            isRetriableMethod = errorHandler.getClass().getDeclaredMethod("isRetriable", Throwable.class);
            isRetriableMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            log.error(logPrefix +
                    "get isRetriable method of errorHandler error.", e);
        }
    }

    public void stop() {
        log.debug(logPrefix +
                "stop log fetch thread.");
        isRunning = false;
    }

    // stopwatch.stop().durations().statistics().getTotal();

    @Override
    public void run() {

        log.info(logPrefix +
                "split log fetch start. running:{}", logProcessorContext.isRunning());
        Stopwatch connectionUseSw = Stopwatch.accumulating();

        final String query =
                LogMinerQueryBuilder.build(
                        connectorConfig, schema, jdbcConnection.username());
        log.info(logPrefix +
                "log miner query sql:{}", query);

        PreparedStatement miningView = null;
        try {
            miningView = jdbcConnection
                    .connection()
                    .prepareStatement(
                            query,
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY,
                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
            while (logProcessorContext.isRunning() && isRunning) {
                try {

                    ScnSplitInfo splitInfo = pendingLogMinerQueue.poll(period.toMillis(), TimeUnit.MILLISECONDS);
                    if (splitInfo == null) {
                        continue;
                    } else {
                        log.debug(logPrefix +
                                " is fetching the scnSplitInfo:{}, pendingLogMinerQueue:{}", splitInfo, pendingLogMinerQueue.size());
                    }

                    connectionUseSw.start();

                    int retryableIdx = 0;
                    for (; retryableIdx <= LOGMINER_RETEYABLE_NUM; retryableIdx++) {
                        try {
                            if (retryableIdx > 0) {
                                splitInfo.resetLogEvents();
                            }
                            logMiner(miningView, splitInfo);
                            break;
                        } catch (Exception e) {
                            // todo garry 异常处理再测试下，另外针对interrupt exception需要额外注意
                            logMinerException = e;
                            if (retryableIdx == LOGMINER_RETEYABLE_NUM) {
                                log.error(logPrefix +
                                        "Mining session stopped, reached max retry num:{}", LOGMINER_RETEYABLE_NUM, e);
                                throw new RuntimeException(e);
                            }
                            log.error(logPrefix +
                                    "log miner error. fetching the scnSplitInfo:{}, pendingLogMinerQueue:{}", splitInfo, pendingLogMinerQueue.size(), e);
                            boolean retriable = isRetriable(e);
                            streamingMetrics.incrementErrorCount();
                            log.error(logPrefix +
                                    "Mining session stopped, retryable:{}. fetching the scnSplitInfo:{}, pendingLogMinerQueue:{}", retriable,
                                    splitInfo, pendingLogMinerQueue.size());

                            if (!retriable) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    connectionUseSw.stop();
                    // 重新创建连接报错，回来再解决
                    /*Duration connUseTime = connectionUseSw.durations().statistics().getTotal();
                    if (connUseTime.compareTo(SplitLogFetcherManager.resetConnectionTimeout) > 0) {
                        log.debug(logPrefix +
                                "reset connection after {}ms, the resetConnectionTimeout:{}", connUseTime, SplitLogFetcherManager.resetConnectionTimeout);
                        jdbcConnection = SplitLogFetcherManager.resetJdbcConnection(jdbcConnection);
                        miningView = jdbcConnection
                                .connection()
                                .prepareStatement(
                                        query,
                                        ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY,
                                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    }*/

                } catch (InterruptedException e) {
                    log.error(logPrefix +
                            "split log fetch interrupted exception.", e);
                } catch (Exception e) {
                    log.error(logPrefix +
                            "split log fetch exception.", e);
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            logError(streamingMetrics, "Mining session stopped due to the {}", t);
            throw new RuntimeException(t);
        } finally {
            if (miningView != null) {
                try {
                    miningView.close();
                } catch (SQLException e) {
                    log.error(logPrefix + "" +
                            "mining view error.", e);
                }
            }
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                log.error(logPrefix + "" +
                        "close jdbc connection error.", e);
            }
            log.debug("Streaming metrics dump: {}", streamingMetrics.toString());
            log.debug(logPrefix +
                    "split log fetch end.");
        }

    }


    private boolean isRetriable(Throwable throwable) {
        try {
            return (boolean) isRetriableMethod.invoke(errorHandler, throwable);
        } catch (Exception e) {
            log.error(logPrefix +
                    "isRetriableMethod error", e);
        }
        return false;
    }


    private final OracleDatabaseSchema schema;
    private final boolean isRac;
    private final Set<String> racHosts = new HashSet<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final ErrorHandler errorHandler;
    OracleConnectorConfig connectorConfig;
    OracleConnection jdbcConnection;
    private boolean isContinuousMining;
    private OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private Duration archiveLogRetention;
    private boolean archiveLogOnlyMode;
    private String archiveDestinationName;

    private void logMiner(PreparedStatement miningView, ScnSplitInfo splitInfo) throws SQLException, InterruptedException {

        log.debug(logPrefix + "start log miner, spitInfo:{}", splitInfo);
        Stopwatch costSt = Stopwatch.reusable().start();
        int round = 0;

        Scn startScn = splitInfo.getStartScn();
        Scn endScn = splitInfo.getEndScn();
        boolean endScnInRedoLog = splitInfo.isEndScnInRedoLog();
        boolean needProgressEndScn = true;
        LogEvent lastProcessedLogEvent;
        if (connectorConfig.getConfig().getBoolean("backToCDB")){
            setCDBSession(jdbcConnection);
        }

        try {

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

            initializeRedoLogsForMining(jdbcConnection, false, startScn);

            Stopwatch stopwatch = Stopwatch.reusable();
            boolean needContinueLog = false;

            // 不需要包含 start scn
            Scn logMinerStartScn = startScn.add(Scn.valueOf(1));
            Scn queryStartScn = startScn;
            Scn lastProcessedScn = null;
            // 查询start scn对应的 archive log，查询需要在log miner之前
            ArchiveLogPO firstLogMinerStartScnArchiveLogPO = null;
            do {

                // Calculate time difference before each mining session to detect time
                // zone offset changes (e.g. DST) on database server
                streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));
                // 查询数据之前判断是否还需要继续logminer。 eg： 日志文件是redo log 文件
                needContinueLog = needContinueLogMiner(endScn);

                log.debug(logPrefix +
                        "log fetch, needContinueLog:{}, endScnInRedoLog:{}", needContinueLog, endScnInRedoLog);
                if (!needContinueLog && endScnInRedoLog) {
                    // end scn 已经不在redo中，可以通知 split assign 线程进行分片了
                    splitInfo.endScnRedoLogCountDown(endScn);
                }

                log.debug(logPrefix +
                                "logminer, start round," +
                                "round: {}, startScn: {} , endScn: {} ,endScn-startScn = {},needContinueLog:{} , splitInfo:{}",
                        round++ ,startScn, endScn, endScn.subtract(startScn),  needContinueLog ,splitInfo);

                initializeRedoLogsForMining(jdbcConnection, true, startScn);

                if (endScnInRedoLog) {
                    firstLogMinerStartScnArchiveLogPO = LogMinerRepository.seekArchiveLog(jdbcConnection, startScn.add(Scn.valueOf(1)));
                }

                startLogMining(
                        jdbcConnection,
                        logMinerStartScn,
                        endScn,
                        strategy,
                        isContinuousMining,
                        streamingMetrics);

                miningView.setFetchSize(connectorConfig.getConfig().getInteger("miningviewFetchSize"));

                miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                miningView.setString(1, queryStartScn.toString());
                miningView.setString(2, endScn.toString());

                stopwatch.start();
                try (ResultSet rs = miningView.executeQuery()) {
                    Duration lastDurationOfBatchCapturing =
                            stopwatch.stop().durations().statistics().getTotal();
                    streamingMetrics.setLastDurationOfBatchCapturing(
                            lastDurationOfBatchCapturing);

                    stopwatch.start();
                    lastProcessedLogEvent = processResult(rs, splitInfo);
                    if (lastProcessedLogEvent != null) {
                        splitInfo.setLastLogEvent(lastProcessedLogEvent);
                        lastProcessedScn = splitInfo.getLastProcessedScn();
                        if (lastProcessedScn != null) {
                            logMinerStartScn = lastProcessedScn.add(Scn.valueOf(1));
                            queryStartScn = lastProcessedScn;

                            /*if (lastProcessedScn.compareTo(endScn) >= 0 && !endScnInRedoLog) {
                                log.debug(logPrefix +
                                        "the lastProcessScn is eq endScn, no need to continue log miner. lastProcessScn:{}, endScn:{}", lastProcessedScn, endScn);
                                needContinueLog = false;
                            }*/
                        }
                    }

                }

                stopwatch.stop();
                log.debug(logPrefix +
                                "logminer, finished round:{}" +
                                "startScn: {} , endScn: {} ,endScn-startScn = {},lastProcessScn={}, cost:{} ms,needContinueLog:{}, splitInfo:{}",
                        round, startScn, endScn, endScn.subtract(startScn),
                        splitInfo.getLastProcessedScn(),
                        stopwatch.durations().statistics().getTotal().toMillis(),
                        needContinueLog, splitInfo);
                log.debug(logPrefix +
                        "streaming metrics:{}", streamingMetrics);

                if (needContinueLog && lastProcessedLogEvent == null) {
                    // 需要持续logminer，且没有查询到数据
                    pauseBetweenMiningSessions(round);
                }

                if (needContinueLog && needProgressEndScn) {
                    Scn addBatchSizeScn = endScn.add(Scn.valueOf(connectorConfig.getLogMiningBatchSizeMin()));
                    int logMiningBatchSizeMax = connectorConfig.getLogMiningBatchSizeMax();
                    log.debug("needContinueLog: {}  ,needProgressEndScn: {} ,addBatchSizeScn.longValue() - startScn.longValue() = {}",needContinueLog,needProgressEndScn,addBatchSizeScn.longValue() - startScn.longValue());
                    if (addBatchSizeScn.longValue() - queryStartScn.longValue() <= logMiningBatchSizeMax) {
                        // 往前递进
                        endScn = addBatchSizeScn;
                        Scn currentScn = jdbcConnection.getCurrentScn();
                        if (endScn.longValue() > currentScn.longValue()) {
                            log.debug("the calculated end scn is gt current scn, use the current scn as end scn. calculated scn:{}, current scn:{}",
                                    endScn, currentScn);
                            endScn = currentScn;
                        }
                    } else {
                        // 单前分片range已经大于最大值，，需要通知继续分片，并且此分片不再继续往前递进
                        log.debug(logPrefix +
                                "the current split range is gt max range, need to continue split. current range:{}, max range:{}, endScn:{}",
                                addBatchSizeScn.longValue() - startScn.longValue(), logMiningBatchSizeMax, endScn);
                        needProgressEndScn = false;
                        splitInfo.endScnRedoLogCountDown(endScn);
                    }

                }

                endMining(jdbcConnection);
                //end scn一直往前走，，且值不会大于 current scn，，这样我们再去根据这个 end scn判断archive log是否在
                // archive log，就有可能一直不在，因为你这个end scn 一直有往前走

                // 是否需要通知 继续分片，，我们还需要加一个逻辑，start scn + 1 对应的log 被archive ，我们也停止解析，
                // 并通知 继续分片，end scn=这个 start scn对应的 archive log end scn - 1
                // 当然还需要与 last processed scn比较

                if (endScnInRedoLog && firstLogMinerStartScnArchiveLogPO != null) {
                    // exclusive
                    long archiveLogNextScn = firstLogMinerStartScnArchiveLogPO.getEndScn();
                    // inclusive
                    Scn logLastEventScn = Scn.valueOf(archiveLogNextScn - 1);
                    log.debug(logPrefix + "endScnInRedoLog = {} archiveLogNextScn = {} logLastEventScn = {} ",endScnInRedoLog,archiveLogNextScn,logLastEventScn);
                    // logMinerStartScn
                    if (lastProcessedScn != null && lastProcessedScn.compareTo(logLastEventScn) > 0) {
                        log.debug(logPrefix +
                                        "the start scn log has been archived, need to terminate the current logminer. start scn:{}, last processed scn:{}, end scn:{}",
                                logMinerStartScn, lastProcessedScn, endScn);
                        endScn = lastProcessedScn;
                    } else {
                        log.debug(logPrefix +
                                        "the start scn log has been archived, need to terminate the current logminer . start scn:{}, log last scn:{}, end scn:{}",
                                logMinerStartScn, logLastEventScn, endScn);
                        endScn = logLastEventScn;
                    }
                    needContinueLog = false;

                }

            } while (logProcessorContext.isRunning() && needContinueLog);

            if (logProcessorContext.isRunning()) {
                splitInfo.endScnRedoLogCountDown(endScn);
                markSplitFinished(splitInfo);
            }

            log.debug(logPrefix +
                    "end log miner cost {} ms, total round:{}, splitInfo:{}",
                    costSt.stop().durations().statistics().getTotal(), round, splitInfo);

        } finally {
            log.debug(logPrefix +
                    "Streaming metrics dump: {}", streamingMetrics.toString());
        }
    }
    static void setCDBSession(JdbcConnection connection) throws SQLException {
        // PDB数据库不支持redolog程序包，因此需要切换到CDB数据库进行读取
        connection.executeWithoutCommitting("alter session set container=CDB$ROOT");
    }
    private void pauseBetweenMiningSessions(int round) throws InterruptedException {
        if (round <= 0) {
            round = 1;
        }
        if (round > logProcessorContext.getPauseBetweenContinueLogMinerMultiplier()) {
            round = logProcessorContext.getPauseBetweenContinueLogMinerMultiplier();
        }
        Duration period = Duration.ofMillis(logProcessorContext.getPauseBetweenContinueLogMiner() * round);
        Metronome.sleeper(period, Clock.SYSTEM).pause();
    }

    private void markSplitFinished(ScnSplitInfo splitInfo) {
        splitInfo.finish(logPrefix);
    }

    private void initializeRedoLogsForMining(
            OracleConnection connection, boolean postEndMiningSession, Scn startScn)
            throws SQLException {
        if (!postEndMiningSession) {
            if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                buildDataDictionary(connection);
            }
            if (!isContinuousMining) {
                setLogFilesForMining(
                        connection,
                        startScn,
                        archiveLogRetention,
                        archiveLogOnlyMode,
                        archiveDestinationName);
            }
        } else {
            if (!isContinuousMining) {
                if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    buildDataDictionary(connection);
                }
                setLogFilesForMining(
                        connection,
                        startScn,
                        archiveLogRetention,
                        archiveLogOnlyMode,
                        archiveDestinationName);
            }
        }
    }

    /**
     * （start scn, end scn] -- [first scn, next scn)
     *
     * @param endScn
     * @return
     */
    private boolean needContinueLogMiner(Scn endScn) {
        boolean scnInArchiveLog = LogMinerRepository.isScnInArchiveLog(jdbcConnection, endScn);
        if (scnInArchiveLog) {
            log.debug(logPrefix +
                    "the end scn is in the archive log, no need continue log miner, end scn:{}", endScn);
            return false;
        }

        String logSql = "select ARCHIVED,STATUS\n" +
                " from v$log\n" +
                " where first_change# <= " + endScn.longValue() +
                " and next_change# >  " + endScn.longValue();


        try {
            String[] redoRs = jdbcConnection.queryAndMap(logSql, rs -> {
                if (rs.next()) {
                    String[] redoLogRs = new String[2];
                    redoLogRs[0] = rs.getString("ARCHIVED");
                    redoLogRs[1] = rs.getString("STATUS");
                    return redoLogRs;
                }
                return null;
            });
            // 肯定在archive log
            if (redoRs == null) {
                // 先判断了是否在 archive log中，不在archive log 中就应该可以查到数据, 可能正在切换中，再执行一次log miner
                log.debug(logPrefix +
                        "the end scn isn't isn't in the archive log and redolog, log miner again, end scn:{}", endScn);
                return true;
            }
            String archived = redoRs[0];
            String status = redoRs[1];

            boolean needContinueLogMiner = false;

            if (StringUtils.equals(archived, "YES")) {
                needContinueLogMiner = false;
            }

            if (StringUtils.equals(status, "CURRENT")) {
                needContinueLogMiner =  true;
            }

            log.debug(logPrefix +
                            "needContinueLogMiner:{}, archived:{}, status:{}, end scn:{}. ",
                    needContinueLogMiner, archived, status, endScn);
            return needContinueLogMiner;

        } catch (Exception e) {
            log.error(logPrefix +
                    "selectRedoLog error", e);
            return true;
        }

    }


    private LogEvent processResult(ResultSet resultSet, ScnSplitInfo splitInfo) throws SQLException, InterruptedException {

        log.debug(logPrefix +
                "star to process result, splitInfo:{}", splitInfo);

        long l1 = System.currentTimeMillis();
        Scn lastProcessedScn = null;
        Stopwatch stopwatch = Stopwatch.reusable().start();

        LogEvent logEvent = null;
        while (logProcessorContext.isRunning() && hasNext(resultSet)) {

            Scn scn = RowMapper.getScn(resultSet);
            if (scn.isNull()) {
                throw new DebeziumException("Unexpected null SCN detected in LogMiner results");
            }

            String tableName = RowMapper.getTableName(resultSet);
            String segOwner = RowMapper.getSegOwner(resultSet);
            int operationCode = RowMapper.getOperationCode(resultSet);
            Timestamp changeTime = RowMapper.getChangeTime(resultSet);
            String txId = RowMapper.getTransactionId(resultSet);
            String operation = RowMapper.getOperation(resultSet);
            String userName = RowMapper.getUsername(resultSet);
            String rowId = RowMapper.getRowId(resultSet);
            int rollbackFlag = RowMapper.getRollbackFlag(resultSet);
            Object rsId = RowMapper.getRsId(resultSet);

            String redoSql = LogMapper.getSqlRedo(resultSet);
            TableId tableId = null;

            if (tableName != null) {
                tableId = RowMapper.getTableId(connectorConfig.getCatalogName(), resultSet);
            }

            long l2 = System.currentTimeMillis();
            long l3 = l2 - l1;
            /*//2020.1.13 每分钟打印一次
            if (l3 >= 60000 * 2) {
                l1 = l2;
                log.debug(logPrefix +
                                "processResult scn={}, operationCode={}, operation={}, table={}, segOwner={}, userName={}, rowId={}, rollbackFlag={}", scn, operationCode, operation,
                        tableName, segOwner, userName, rowId, rollbackFlag);
            }*/

            lastProcessedScn = scn;

            if (!isRelevantTableEvent(operationCode, segOwner, tableName)) {
                log.debug("ignore irrelevant record, operationCode {}, sgOwner {}, tableName {}, scn {}", operationCode, segOwner, tableName, scn);
                continue;
            }

            logEvent = new LogEvent();
            logEvent.setTableName(tableName);
            logEvent.setSegOwner(segOwner);
            logEvent.setOperationCode(operationCode);
            logEvent.setChangeTime(changeTime);
            logEvent.setTxId(txId);
            logEvent.setOperation(operation);
            logEvent.setUserName(userName);
            logEvent.setRowId(rowId);
            logEvent.setRollbackFlag(rollbackFlag);
            logEvent.setRsId(rsId);
            logEvent.setRedoSql(redoSql);
            logEvent.setScn(scn);
            logEvent.setTableId(tableId);
            splitInfo.addLogEvent(logEvent, logPrefix);

        }

        if (lastProcessedScn != null) {
            splitInfo.setLastProcessedScn(lastProcessedScn);
        }

        stopwatch.stop();
        log.debug(logPrefix +
                "end process result, duration:{}", stopwatch.durations());

        return logEvent;

    }

    private boolean hasNext(ResultSet resultSet) throws SQLException {
        Instant rsNextStart = Instant.now();
        if (resultSet.next()) {
            streamingMetrics.addCurrentResultSetNext(Duration.between(rsNextStart, Instant.now()));
            return true;
        }
        return false;
    }

    private boolean isRelevantTableEvent(int operationCode, String tableOwner, String tableName) {

        // only ddl, insert, update, delete
        switch (operationCode) {
            case RowMapper.DDL:
            case RowMapper.INSERT:
            case RowMapper.UPDATE:
            case RowMapper.DELETE:
                // skip event
                if (StringUtils.isEmpty(tableOwner) || StringUtils.isEmpty(tableName)) {
                    return false;
                }
                if (tableIncludeSet.contains(StringUtils.lowerCase(StringUtils.join(tableOwner, ".", tableName)))) {
                    return true;
                } else {
                    return false;
                }
            default:
                return true;
        }

    }
}
