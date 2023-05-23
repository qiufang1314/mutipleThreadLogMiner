/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.*;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@NotThreadSafe
public class SignalBasedIncrementalSnapshotChangeEventSource<T extends DataCollectionId> implements IncrementalSnapshotChangeEventSource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalBasedIncrementalSnapshotChangeEventSource.class);

    private Map<Struct, Object[]> window = new LinkedHashMap<>();
    private Map<Struct, Object[]> deletewindow = new LinkedHashMap<>();
    private HashMap<String, String> primaryKV = new HashMap<>();
    private CommonConnectorConfig connectorConfig;
    private JdbcConnection jdbcConnection;
    private final Clock clock;
    private String signalWindowStatement = null;
    private final RelationalDatabaseSchema databaseSchema;
    private final SnapshotProgressListener progressListener;
    private final DataChangeEventListener dataListener;
    public  final String executeError = "execute-error";
    private long totalRowsScanned = 0;
    public OffsetContext offsetContext = null;
    private Table currentTable;
    private EventDispatcher<T> dispatcher;
    private IncrementalSnapshotContext<T> context = null;

    public SignalBasedIncrementalSnapshotChangeEventSource(CommonConnectorConfig config, JdbcConnection jdbcConnection,
                                                           DatabaseSchema<?> databaseSchema, Clock clock, SnapshotProgressListener progressListener,
                                                           DataChangeEventListener dataChangeEventListener, EventDispatcher<T> dispatcher) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        //todo cj
        if (connectorConfig.getSignalingDataCollectionId()!=null){
            signalWindowStatement = "INSERT INTO " + (connectorConfig.getSignalingDataCollectionId().split("\\.").length>2
                    ?connectorConfig.getSignalingDataCollectionId().substring(connectorConfig.getSignalingDataCollectionId().indexOf(".")+1)
                    :connectorConfig.getSignalingDataCollectionId()) + "(ID,\"TYPE\",\"DATA\")"
                    + " VALUES (?, ?, ?)";
        }
        this.databaseSchema = (RelationalDatabaseSchema) databaseSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
        this.dispatcher = dispatcher;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(String id, EventDispatcher<T> dispatcher, OffsetContext offsetContext) throws InterruptedException {
        return;

    }
    public void closeWindow( EventDispatcher<T> dispatcher, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();

        LOGGER.info("Sending {} events from window buffer", window.size());
        LOGGER.info("Sending {} events from deletewindow buffer", deletewindow.size());
        offsetContext.incrementalSnapshotEvents();

        for (Object[] delRow : deletewindow.values()) {
            LOGGER.info("delrow ={}", Arrays.toString(Arrays.stream(delRow).toArray()));

            sendDelEvent(dispatcher, offsetContext, delRow);
        }
        for (Object[] row : window.values()) {
            LOGGER.info("row ={}", Arrays.toString(Arrays.stream(row).toArray()));
            sendEvent(dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        deletewindow.clear();
        window.clear();
        context.closeWindow(context.currentChunkId());
        nextDataCollection();

    }

    protected void sendEvent(EventDispatcher<T> dispatcher, OffsetContext offsetContext, Object[] row) throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        LOGGER.info("sendEvent context.currentDataCollectionId() = {}",context.currentDataCollectionId());
        offsetContext.event((T) context.currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent((T) context.currentDataCollectionId(),
                getChangeRecordEmitter(context.currentDataCollectionId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }
    protected void sendDelEvent(EventDispatcher<T> dispatcher, OffsetContext offsetContext, Object[] row) throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        LOGGER.info("sendDelEvent context.currentDataCollectionId() = {}",context.currentDataCollectionId());

        offsetContext.event((T) context.currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent((T) context.currentDataCollectionId(),
                getDelRecordEmitter(context.currentDataCollectionId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(T dataCollectionId, OffsetContext offsetContext,
                                                         Object[] row) {
        return new SnapshotChangeRecordEmitter(offsetContext, row, clock);
    }
    protected ChangeRecordEmitter getDelRecordEmitter(T dataCollectionId, OffsetContext offsetContext,
                                                         Object[] row) {
        return new SnapshotDelRecordEmitter(offsetContext, row, clock);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        if (!context.deduplicationNeeded() || (window.isEmpty() && deletewindow.isEmpty())) {
            return;
        }
        if (context.currentDataCollectionId()!= null && !context.currentDataCollectionId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            if (window.remove((Struct) key) != null) {
                LOGGER.info("Removed '{}' from window", key);
            }
            if (deletewindow.remove((Struct) key) != null) {
                LOGGER.info("Removed '{}' from window", key);
            }

        }
    }

    private void emitWindowOpen() throws SQLException {
        context.openWindow(context.currentChunkId());
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, context.currentChunkId() + "-open");
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
            x.setObject(3,context.currentRawData().orElse(null));
        });
    }

    private void emitWindowClose() throws SQLException {

        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, context.currentChunkId() + "-close");
            x.setString(2, CloseIncrementalSnapshotWindow.NAME);
            x.setObject(3,context.currentRawData().orElse(null));
        });

    }

//    private void emitExecuteError() throws SQLException {
//
//        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
//            x.setString(1, context.currentChunkId() + "-error");
//            x.setString(2, executeError);
//            x.setObject(3,context.currentRawData().orElse(null));
//        });
//    }

    protected String buildChunkQuery(Table table) {
        String condition = null;
        // Add condition when this is not the first query
        if (context.isNonInitialChunk()) {
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addLowerBound(table, sql);
            // Table boundaries
            sql.append(" AND NOT ");
            addLowerBound(table, sql);
            condition = sql.toString();
        }
        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelectWithRowLimits(table.id(),
                connectorConfig.getIncrementalSnashotChunkSize(),
                "*",
                Optional.ofNullable(condition),
                orderBy);
    }

    protected String buildChunkQuery(Table table, Optional<String> additionalCondition) {
        String condition = null;

        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelect(table.id(),
                connectorConfig.getIncrementalSnashotChunkSize(),
                "*",
                Optional.ofNullable(condition),
                additionalCondition,
                orderBy);
    }
    private void addLowerBound(Table table, StringBuilder sql) {
        // To make window boundaries working for more than one column it is necessary to calculate
        // with independently increasing values in each column independently.
        // For one column the condition will be (? will always be the last value seen for the given column)
        // (k1 > ?)
        // For two columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?)
        // For four columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 > ?) OR (k1 = ? AND k2 = ? AND k3 = ? AND k4 > ?)
        // etc.
        final List<Column> pkColumns = table.primaryKeyColumns();
        if (pkColumns.size() > 1) {
            sql.append('(');
        }
        for (int i = 0; i < pkColumns.size(); i++) {
            final boolean isLastIterationForI = (i == pkColumns.size() - 1);
            sql.append('(');
            for (int j = 0; j < i + 1; j++) {
                final boolean isLastIterationForJ = (i == j);
                sql.append(pkColumns.get(j).name());
                sql.append(isLastIterationForJ ? " > ?" : " = ?");
                if (!isLastIterationForJ) {
                    sql.append(" AND ");
                }
            }
            sql.append(")");
            if (!isLastIterationForI) {
                sql.append(" OR ");
            }
        }
        if (pkColumns.size() > 1) {
            sql.append(')');
        }
    }

    protected String buildMaxPrimaryKeyQuery(Table table) {
        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelectWithRowLimits(table.id(), 1, "*", Optional.empty(), orderBy.toString());
    }

    //todo cj add condition
    protected String buildMaxPrimaryKeyQuery(Table table,Optional<String> additionalCondition) {
        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelect(table.id(), 1, "*", Optional.empty(), additionalCondition, orderBy.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(OffsetContext offsetContext) {
        if (offsetContext == null) {
            LOGGER.info("Empty incremental snapshot change event source started, no action needed");
            return;
        }
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (!context.snapshotRunning()) {
            LOGGER.info("No incremental snapshot in progress, no action needed on start");
            return;
        }
        LOGGER.info("Incremental snapshot in progress, need to read new chunk on start");
        try {
            progressListener.snapshotStarted();
            readChunk();
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    private void readChunk() throws InterruptedException {
        //如果没有数据，并且窗口是开着的，那就返回
        if (!context.snapshotRunning() || context.deduplicationNeeded()) {
            LOGGER.info("!context.snapshotRunning() return readchunk");
            return;
        }
        try {
            LOGGER.info("context.tablesToBeSnapshottedCount() = {} !context.snapshotRunning = {} context.deduplicationNeeded = {}",context.tablesToBeSnapshottedCount(),!context.snapshotRunning() , context.deduplicationNeeded());

            final TableId currentTableId = (TableId) context.currentDataCollectionId();
            if (currentTableId == null){
                LOGGER.info("currentTableId == null ,currentChunkId ={} currentDataCollectionId = {} currentAddtionData = {} currentRawData ={}",
                        context.currentChunkId(),context.currentDataCollectionId(),context.currentAddtionData(),context.currentRawData());
                return;
            }
            jdbcConnection.commit();
            currentTable = databaseSchema.tableFor(currentTableId);
            LOGGER.info("context.snapshotRunning() start readChunk");

            if (currentTable == null) {
                LOGGER.warn("Schema not found for table '{}', known tables {}", currentTableId, databaseSchema.tableIds());
//                emitExecuteError();
                nextDataCollection();
                return;
            }
            if (currentTable.primaryKeyColumns().isEmpty()) {
                LOGGER.warn("Incremental snapshot for table '{}' skipped cause the table has no primary keys", currentTableId);
//                emitExecuteError();
                nextDataCollection();
                return;
            }
            Boolean isAdditionCorrect = initPrimaryKV();
            if (!isAdditionCorrect){
//                emitExecuteError();
                nextDataCollection();
                return;
            }
            context.startNewChunk();
            emitWindowOpen();
            jdbcConnection.commit();

            if (!context.maximumKey().isPresent()) {
                context.maximumKey(jdbcConnection.queryAndMap(buildMaxPrimaryKeyQuery(currentTable,context.currentAddtionData()), rs -> {
                    if (!rs.next()) {
                        return keyFromRow(jdbcConnection.emptyRowToArray(currentTable, databaseSchema, rs,
                                ColumnUtils.toArray(rs, currentTable),primaryKV));
                    }
                    return keyFromRow(jdbcConnection.rowToArray(currentTable, databaseSchema, rs,
                            ColumnUtils.toArray(rs, currentTable)));
                }));
            }
            createDataEventsForTable();
            if (window.isEmpty() && deletewindow.isEmpty()) {
                LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                        currentTableId);
                tableScanCompleted();
                emitWindowClose();
                jdbcConnection.commit();
                context.closeWindow(context.currentChunkId());
                nextDataCollection();
            }
            emitWindowClose();
            jdbcConnection.commit();
            closeWindow(this.dispatcher,this.offsetContext);
        }
        catch (SQLException e) {
            throw new DebeziumException(String.format("Database error while executing incremental snapshot for table '%s'", context.currentDataCollectionId()), e);
        }
    }

    private void nextDataCollection() throws InterruptedException {
        context.nextDataCollection();
        context.nextAdditionData();
        context.nextRawData();
        readChunk();

        if (!context.snapshotRunning()) {
            progressListener.snapshotCompleted();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        boolean shouldReadChunk = false;
        if (!context.snapshotRunning()) {
            shouldReadChunk = true;
        }
        final List<T> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds);
        if (shouldReadChunk) {
            progressListener.snapshotStarted();
            progressListener.monitoredDataCollectionsDetermined(newDataCollectionIds);
            readChunk();
        }
    }
    //todo 增量快照加上sql条件
    @Override
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> additionalCondition, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        boolean shouldReadChunk = false;
        if(context==null){
            context= new IncrementalSnapshotContext<>();
//            shouldReadChunk = true;
        }
        if (!context.snapshotRunning()) {
            shouldReadChunk = true;
        }
        final List<T> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds,additionalCondition);
        if (shouldReadChunk) {
            progressListener.snapshotStarted();
            progressListener.monitoredDataCollectionsDetermined(newDataCollectionIds);
            readChunk();
        }
    }
    // todo jace 加入数据
    @Override
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> additionalCondition, OffsetContext offsetContext, String data) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        this.offsetContext = offsetContext;
        boolean shouldReadChunk = false;
        if(context==null){
            context= new IncrementalSnapshotContext<>();
        }

        if (!context.snapshotRunning()) {
            shouldReadChunk = true;
        }
        LOGGER.info("addDataCollectionNamesToSnapshot dataCollectionIds = {} additionalCondition = {}  data = {} shouldReadChunk = {}", Arrays.toString(dataCollectionIds.toArray()),additionalCondition.get(),data,shouldReadChunk);
        final List<T> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds,additionalCondition,data);
        if (shouldReadChunk) {
            progressListener.snapshotStarted();
            progressListener.monitoredDataCollectionsDetermined(newDataCollectionIds);
            readChunk();
        }
    }

    protected void addKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = table.primaryKeyColumns().iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append(key.name()).append(predicate);
            if (i.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForTable() throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.tablesToBeSnapshottedCount());

        final String selectStatement = buildChunkQuery(currentTable,context.currentAddtionData());
        LOGGER.debug("\t For table '{}' using select statement: '{}', key: '{}'", currentTable.id(),
                selectStatement, context.chunkEndPosititon(), context.maximumKey().get());

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());

        try (PreparedStatement statement = readTableChunkStatement(selectStatement);
                ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();

            Object[] lastRow = null;
            Object[] firstRow = null;
            LOGGER.info("use select Statement={}" ,selectStatement);
            while (rs.next()) {
                rows++;
                final Object[] row = jdbcConnection.rowToArray(currentTable, databaseSchema, rs, columnArray);
//                if (firstRow == null) {
//                    firstRow = row;
//                }
                final Struct keyStruct = tableSchema.keyFromColumnData(row);
                window.put(keyStruct, row);
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.debug("\t Exported {} records for table '{}' after {}", rows, currentTable.id(),
                            Strings.duration(stop - exportStart));
                    logTimer = getTableScanLogTimer();
                }
//                lastRow = row;
            }
            // todo jace 处理删除逻辑
            if (rows==0){
                rows++;
                Object[] deleterow = jdbcConnection.emptyRowToArray(currentTable, databaseSchema, rs, columnArray, primaryKV);
//                if (firstRow == null) {
//                    firstRow = deleterow;
//                }
                final Struct keyStruct = tableSchema.keyFromColumnData(deleterow);
                deletewindow.put(keyStruct,deleterow);
//                lastRow = deleterow;
            }

//            final Object[] firstKey = keyFromRow(firstRow);
//            final Object[] lastKey = keyFromRow(lastRow);
//            context.nextChunkPosition(lastKey);
//            progressListener.currentChunk(context.currentChunkId(), firstKey, lastKey);
//            if (lastRow != null) {
//                LOGGER.debug("\t Next window will resume from '{}'", (Object) context.chunkEndPosititon());
//            }

            LOGGER.debug("\t Finished exporting {} records for window of table table '{}'; total duration '{}'", rows,
                    currentTable.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            incrementTableRowsScanned(rows);
        }
        catch (SQLException e) {
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
        }
    }
    private Boolean initPrimaryKV(){
        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());
        primaryKV.clear();
        if(context.currentAddtionData().isPresent()){
            String[] primaryKey = context.currentAddtionData().get().split("and");
            for (int i = 0; i < primaryKey.length; i++) {
                String[] kv = primaryKey[i].split("=");
                if (kv.length== 2){
                    Field field = tableSchema.valueSchema().field(kv[0].trim().toUpperCase());
                    if(field!= null){
                        primaryKV.put(kv[0].trim(),kv[1].trim().replace("'",""));
                        LOGGER.info("kv[0] = {},kv[1] ={}" ,kv[0].trim(),kv[1].trim());
                    }else {
                        LOGGER.info(" validate field error with = {} ",context.currentAddtionData().get());
                        return false;
                    }
                }else {
                    LOGGER.info(" additionalData error with = {} ",context.currentAddtionData().get());
                    return false;
                }
            }
        }

        return true;
    }

    private void incrementTableRowsScanned(long rows) {
        totalRowsScanned += rows;
        progressListener.rowsScanned(currentTable.id(), totalRowsScanned);
    }

    private void tableScanCompleted() {
        progressListener.dataCollectionSnapshotCompleted(currentTable.id(), totalRowsScanned);
        totalRowsScanned = 0;
    }

    protected PreparedStatement readTableChunkStatement(String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.readTablePreparedStatement(connectorConfig, sql,
                OptionalLong.empty());
//        if (context.isNonInitialChunk()) {
//            final Object[] maximumKey = context.maximumKey().get();
//            final Object[] chunkEndPosition = context.chunkEndPosititon();
//            // Fill boundaries placeholders
//            int pos = 0;
//            for (int i = 0; i < chunkEndPosition.length; i++) {
//                for (int j = 0; j < i + 1; j++) {
//                    statement.setObject(++pos, chunkEndPosition[j]);
//                }
//            }
//            // Fill maximum key placeholders
//            for (int i = 0; i < chunkEndPosition.length; i++) {
//                for (int j = 0; j < i + 1; j++) {
//                    statement.setObject(++pos, maximumKey[j]);
//                }
//            }
//        }
        return statement;
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, RelationalSnapshotChangeEventSource.LOG_INTERVAL);
    }

    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        final List<Column> keyColumns = currentTable.primaryKeyColumns();
        final Object[] key = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            key[i] = row[keyColumns.get(i).position() - 1];
        }
        return key;
    }

    protected void setContext(IncrementalSnapshotContext<T> context) {
        this.context = context;
    }
}
