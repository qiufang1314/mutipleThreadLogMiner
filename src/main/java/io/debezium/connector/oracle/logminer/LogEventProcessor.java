package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.*;
import io.debezium.connector.oracle.logminer.parser.*;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.getLastScnToAbandon;

@Slf4j
public class LogEventProcessor {

    private LogProcessorContext logProcessorContext;

    private TransactionalBuffer transactionalBuffer;
    private ChangeEventSource.ChangeEventSourceContext context;
    private EventDispatcher<TableId> dispatcher;
    private OracleOffsetContext offsetContext;

    private OracleConnectorConfig connectorConfig;

    private OracleDatabaseSchema schema;

    private OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private SelectLobParser selectLobParser;
    private DmlParser dmlParser;
    OracleConnection jdbcConnection;

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private Scn lastProcessedScn = Scn.NULL;
    private long stuckScnCounter = 0;

    public LogEventProcessor(LogProcessorContext logProcessorContext,
                             ChangeEventSource.ChangeEventSourceContext context, OracleConnectorConfig connectorConfig,
                             OracleStreamingChangeEventSourceMetrics streamingMetrics, TransactionalBuffer transactionalBuffer,
                             OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                             EventDispatcher<TableId> dispatcher,OracleConnection jdbcConnection) {
        this.logProcessorContext = logProcessorContext;
        this.jdbcConnection = jdbcConnection;
        this.context = context;
        this.streamingMetrics = streamingMetrics;
        this.transactionalBuffer = transactionalBuffer;
        this.offsetContext = offsetContext;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.connectorConfig = connectorConfig;
        this.dmlParser = resolveParser(connectorConfig, schema.getValueConverters());
        this.selectLobParser = new SelectLobParser();
    }

    private void log(boolean needPrintLog, String format, Object ... args) {
        if (needPrintLog) {
            log.info(format, args);
        }
    }

    /**
     * This method does all the job
     * @throws SQLException thrown if any database exception occurs
     */
    void processResult(LogEventBatch currentLogEventBatch, boolean needPrintLog) throws SQLException {

        List<LogEvent> logEvents = currentLogEventBatch.getLogEvents();

        int dmlCounter = 0, insertCounter = 0, updateCounter = 0, deleteCounter = 0;
        int commitCounter = 0;
        int rollbackCounter = 0;
        long rows = 0;
        Instant startTime = Instant.now();
        Stopwatch stopwatch = Stopwatch.reusable().start();
        log(needPrintLog,"start to process log events, currentLogEventBatch:{}", currentLogEventBatch);

        long l1 = System.currentTimeMillis();

        Iterator<LogEvent> logIterator = logEvents.iterator();

        while (logProcessorContext.isRunning() && logIterator.hasNext()) {

            LogEvent logEvent = logIterator.next();
            rows++;

            String tableName = logEvent.getTableName();
            String segOwner = logEvent.getSegOwner();
            int operationCode = logEvent.getOperationCode();
            Timestamp changeTime = logEvent.getChangeTime();
            String txId = logEvent.getTxId();
            String operation = logEvent.getOperation();
            String userName = logEvent.getUserName();
            String rowId = logEvent.getRowId();
            int rollbackFlag = logEvent.getRollbackFlag();
            Object rsId = logEvent.getRsId();
            boolean dml = isDmlOperation(operationCode);
            Scn scn = logEvent.getScn();
            TableId tableId = logEvent.getTableId();

            String redoSql = logEvent.getRedoSql();

            // todo copy from...
            long l2 = System.currentTimeMillis();
            long l3 = l2- l1;
            //2020.1.13 每分钟打印一次
            if(l3>=60000 * 2){
                l1 = l2;
                log.info("processResult scn={}, operationCode={}, operation={}, table={}, segOwner={}, userName={}, rowId={}, rollbackFlag={}", scn, operationCode, operation,
                        tableName, segOwner, userName, rowId, rollbackFlag);
            }

            String logMessage = String.format("transactionId=%s, SCN=%s, table_name=%s, segOwner=%s, operationCode=%s, offsetSCN=%s, " +
                    " commitOffsetSCN=%s", txId, scn, tableName, segOwner, operationCode, offsetContext.getScn(), offsetContext.getCommitScn());


            switch (operationCode) {
                case RowMapper.START: {
                    // Register start transaction.
                    // If already registered, does nothing due to overlapping mining strategy.
                    transactionalBuffer.registerTransaction(txId, scn);
                    break;
                }
                case RowMapper.COMMIT: {
                    // Commits a transaction
                    if (transactionalBuffer.isTransactionRegistered(txId)) {
                        if (transactionalBuffer.commit(txId, scn, offsetContext, changeTime, context, logMessage, dispatcher)) {
//                            log.trace("COMMIT, {}", logMessage);
                            commitCounter++;
                        }
                    }
                    break;
                }
                case RowMapper.ROLLBACK: {
                    // Rollback a transaction
                    if (transactionalBuffer.isTransactionRegistered(txId)) {
                        if (transactionalBuffer.rollback(txId, logMessage)) {
                            log.trace("ROLLBACK, {}", logMessage);
                            rollbackCounter++;
                        }
                    }
                    break;
                }
                case RowMapper.DDL: {
                    if (transactionalBuffer.isDdlOperationRegistered(scn)) {
                        log.trace("DDL: {} has already been seen, skipped.", redoSql);
                        continue;
                    }
                    if(l3>=60000 * 2) {
                        log.info("DDL: {}, REDO_SQL: {}", logMessage, redoSql);
                    }
                    try {
                        if (tableName != null) {
                            transactionalBuffer.registerDdlOperation(scn);
                            dispatcher.dispatchSchemaChangeEvent(tableId,
                                    new OracleSchemaChangeEventEmitter(
                                            connectorConfig,
                                            offsetContext,
                                            tableId,
                                            tableId.catalog(),
                                            tableId.schema(),
                                            redoSql,
                                            schema,
                                            changeTime.toInstant(),
                                            streamingMetrics));
                        }
                    }
                    catch (InterruptedException e) {
                        throw new DebeziumException("Failed to dispatch DDL event", e);
                    }
                }
                case RowMapper.SELECT_LOB_LOCATOR: {
                    if (!connectorConfig.isLobEnabled()) {
                        log.trace("SEL_LOB_LOCATOR operation skipped for '{}', LOB not enabled.", redoSql);
                        continue;
                    }
                    log.trace("SEL_LOB_LOCATOR: {}, REDO_SQL: {}", logMessage, redoSql);
                    final Table table = schema.tableFor(tableId);
                    if (table == null) {
                        LogMinerHelper.logWarn(streamingMetrics, "SEL_LOB_LOCATOR for table '{}' is not known to the connector, skipped.", tableId);
                        continue;
                    }
                    transactionalBuffer.registerSelectLobOperation(operationCode, txId, scn, tableId, changeTime.toInstant(),
                            rowId, rsId, segOwner, tableName, redoSql, schema.tableFor(tableId), selectLobParser);
                    break;
                }
                case RowMapper.LOB_WRITE: {
                    if (!connectorConfig.isLobEnabled()) {
                        log.trace("LOB_WRITE operation skipped, LOB not enabled.");
                        continue;
                    }
                    if (schema.tableFor(tableId) == null) {
                        LogMinerHelper.logWarn(streamingMetrics, "LOB_WRITE for table '{}' is not known to the connector, skipped.", tableId);
                        continue;
                    }
                    transactionalBuffer.registerLobWriteOperation(operationCode, txId, scn, tableId, redoSql,
                            changeTime.toInstant(), rowId, rsId);
                    break;
                }
                case RowMapper.LOB_ERASE: {
                    if (!connectorConfig.isLobEnabled()) {
                        log.trace("LOB_ERASE operation skipped, LOB not enabled.");
                        continue;
                    }
                    if (schema.tableFor(tableId) == null) {
                        LogMinerHelper.logWarn(streamingMetrics, "LOB_ERASE for table '{}' is not known to the connector, skipped.", tableId);
                        continue;
                    }
                    transactionalBuffer.registerLobEraseOperation(operationCode, txId, scn, tableId, changeTime.toInstant(), rowId, rsId);
                    break;
                }
                case RowMapper.INSERT:
                case RowMapper.UPDATE:
                case RowMapper.DELETE: {
                    log.trace("DML, {}, sql {}", logMessage, redoSql);
                    if (redoSql != null) {
                        dmlCounter++;
                        switch (operationCode) {
                            case RowMapper.INSERT:
                                insertCounter++;
                                break;
                            case RowMapper.UPDATE:
                                updateCounter++;
                                break;
                            case RowMapper.DELETE:
                                deleteCounter++;
                                break;
                        }

                        final Table table = getTableForDmlEvent(tableId);

                        if (rollbackFlag == 1) {
                            // DML operation is to undo partial or all operations as a result of a rollback.
                            // This can be situations where an insert or update causes a constraint violation
                            // and a subsequent operation is written to the logs to revert the change.
                            transactionalBuffer.undoDmlOperation(txId, rowId, tableId);
                            continue;
                        }

                        transactionalBuffer.registerDmlOperation(operationCode, txId, scn, tableId, () -> {
                                    final LogMinerDmlEntry dmlEntry = parse(redoSql, table, txId);
                                    dmlEntry.setObjectOwner(segOwner);
                                    dmlEntry.setObjectName(tableName);
                                    return dmlEntry;
                                },
                                changeTime.toInstant(), rowId, rsId);
                    }
                    else {
                        log.trace("Redo SQL was empty, DML operation skipped.");
                    }

                    break;
                }
            }
        }

        // 设置scn，checkpoint
        final Scn minStartScn = transactionalBuffer.getMinimumScn();
        if (!minStartScn.isNull()) {
            long printTime = System.currentTimeMillis() % 30;
            if (printTime==0){
                log.info("LogEventProcessor processResult set offsetContext.setSCN() to transactionalBuffer.getMinimumScn() = {}" ,minStartScn);
            }
            offsetContext.setScn(
                    minStartScn.subtract(Scn.valueOf(1)));
        }

        if (currentLogEventBatch.isSplitScnFetchFinished()) {
            if (transactionalBuffer.isEmpty()) {
                long printTime = System.currentTimeMillis() % 30;
                if (printTime==0){
                    log.info("LogEventProcessor processResult set offsetContext.setSCN() to currentLogEventBatch.getEndScn() = {}" ,currentLogEventBatch.getEndScn());
                }
                offsetContext.setScn(currentLogEventBatch.getEndScn());
            }
        }

        Duration totalTime = Duration.between(startTime, Instant.now());
        if (dmlCounter > 0 || commitCounter > 0 || rollbackCounter > 0) {
            streamingMetrics.setLastCapturedDmlCount(dmlCounter);
            streamingMetrics.setLastDurationOfBatchProcessing(totalTime);

            warnStuckScn();
            currentOffsetScn = offsetContext.getScn();
            if (offsetContext.getCommitScn() != null) {
                currentOffsetCommitScn = offsetContext.getCommitScn();
            }
        }

        stopwatch.stop();
        log(needPrintLog,"process-end: {} Rows, {} DMLs, {} Commits, {} Rollbacks, {} Inserts, {} Updates, {} Deletes. " +
                        "Lag:{}. Offset scn:{}. Offset commit scn:{}. Active transactions:{}.,currentLogEventBatch:{}, durations:{}ms",
                rows, dmlCounter, commitCounter, rollbackCounter, insertCounter, updateCounter, deleteCounter,
                streamingMetrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(), offsetContext.getCommitScn(),
                streamingMetrics.getNumberOfActiveTransactions(),
                currentLogEventBatch,
                stopwatch.durations().statistics().getTotal().toMillis()
        );

        streamingMetrics.addProcessedRows(rows);
        abandonOldTransactionsIfExist(jdbcConnection, offsetContext, transactionalBuffer);
    }
    private void abandonOldTransactionsIfExist(
            OracleConnection connection,
            OracleOffsetContext offsetContext,
            TransactionalBuffer transactionalBuffer) {
        Duration transactionRetention = connectorConfig.getLogMiningTransactionRetention();
//        log.info("abandonOldTransactionsIfExist  offsetContext.getSCN=={}", offsetContext.getScn());

        if (!Duration.ZERO.equals(transactionRetention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions =
                    getLastScnToAbandon(connection, offsetScn, transactionRetention);
            lastScnToAbandonTransactions.ifPresent(
                    thresholdScn -> {
                        transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
                        offsetContext.setScn(thresholdScn);
                        log.info("abandonOldTransactionsIfExist setSCN={}",thresholdScn);
                    });
        }
    }

    private static boolean isDmlOperation(int operationCode) {
        switch (operationCode) {
            case RowMapper.INSERT:
            case RowMapper.UPDATE:
            case RowMapper.DELETE:
                return true;
            default:
                return false;
        }
    }

    private Table getTableForDmlEvent(TableId tableId) throws SQLException {
        Table table = schema.tableFor(tableId);
        if (table == null) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId);
            }
            else {
                LogMinerHelper.logWarn(streamingMetrics, "DML for table '{}' that is not known to this connector, skipping.", tableId);
            }
        }
        return table;
    }

    private Table dispatchSchemaChangeEventAndGetTableForNewCapturedTable(TableId tableId) throws SQLException {
        try {
            log.info("Table {} is new and will be captured.", tableId);
            offsetContext.event(tableId, Instant.now());
            dispatcher.dispatchSchemaChangeEvent(
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            connectorConfig,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            getTableMetadataDdl(tableId),
                            schema,
                            Instant.now(),
                            streamingMetrics));

            return schema.tableFor(tableId);
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Failed to dispatch schema change event", e);
        }
    }

    private String getTableMetadataDdl(TableId tableId) throws SQLException {
        final String pdbName = connectorConfig.getPdbName();
        // A separate connection must be used for this out-of-bands query while processing the LogMiner query results.
        // This should have negligible overhead as this should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), () -> getClass().getClassLoader())) {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            return connection.getTableMetadataDdl(tableId);
        }
    }

    private LogMinerDmlEntry parse(String redoSql, Table table, String txId) {
        LogMinerDmlEntry dmlEntry;
        try {
            Instant parseStart = Instant.now();
            dmlEntry = dmlParser.parse(redoSql, table, txId);
            streamingMetrics.addCurrentParseTime(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            StringBuilder message = new StringBuilder();
            message.append("DML statement couldn't be parsed.");
            message.append(" Please open a Jira issue with the statement '").append(redoSql).append("'.");
            if (OracleConnectorConfig.LogMiningDmlParser.FAST.equals(connectorConfig.getLogMiningDmlParser())) {
                message.append(" You can set internal.log.mining.dml.parser='legacy' as a workaround until the parse error is fixed.");
            }
            throw new DmlParserException(message.toString(), e);
        }

        if (dmlEntry.getOldValues().length == 0) {
            if (RowMapper.UPDATE == dmlEntry.getOperation() || RowMapper.DELETE == dmlEntry.getOperation()) {
                log.warn("The DML event '{}' contained no before state.", redoSql);
                streamingMetrics.incrementWarningCount();
            }
        }

        return dmlEntry;
    }

    /**
     * This method is warning if a long running transaction is discovered and could be abandoned in the future.
     * The criteria is the offset SCN remains the same in 25 mining cycles
     */
    private void warnStuckScn() {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            final Scn scn = offsetContext.getScn();
            final Scn commitScn = offsetContext.getCommitScn();
            if (currentOffsetScn.equals(scn) && !currentOffsetCommitScn.equals(commitScn)) {
                stuckScnCounter++;
                // logWarn only once
                if (stuckScnCounter == 25) {
                    LogMinerHelper.logWarn(streamingMetrics,
                            "Offset SCN {} is not changing. It indicates long transaction(s). " +
                                    "Offset commit SCN: {}",
                            currentOffsetScn, commitScn);
                    streamingMetrics.incrementScnFreezeCount();
                }
            }
            else {
                stuckScnCounter = 0;
            }
        }
    }

    private static DmlParser resolveParser(OracleConnectorConfig connectorConfig, OracleValueConverters valueConverters) {
        if (connectorConfig.getLogMiningDmlParser().equals(OracleConnectorConfig.LogMiningDmlParser.LEGACY)) {
            return new SimpleDmlParser(connectorConfig.getCatalogName(), valueConverters);
        }
        return new LogMinerDmlParser();
    }


}
