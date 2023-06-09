/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.*;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import java.util.Optional;

public class OracleChangeEventSourceFactory implements ChangeEventSourceFactory<OracleOffsetContext> {

    private final OracleConnectorConfig configuration;
    private final OracleConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final OracleTaskContext taskContext;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    public OracleChangeEventSourceFactory(OracleConnectorConfig configuration, OracleConnection jdbcConnection,
                                          ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, OracleDatabaseSchema schema,
                                          Configuration jdbcConfig, OracleTaskContext taskContext,
                                          OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<OracleOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new OracleSnapshotChangeEventSource(configuration, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<OracleOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            OracleOffsetContext offsetContext,
            SnapshotProgressListener snapshotProgressListener,
            DataChangeEventListener dataChangeEventListener) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        // Incremental snapshots requires a secondary database connection
        // This is because Xstream does not allow any work on the connection while the LCR handler may be invoked
        // and LogMiner streams results from the CDB$ROOT container but we will need to stream changes from the
        // PDB when reading snapshot records.
        return Optional.of(new SignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                jdbcConnection,

                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,dispatcher));
    }
}
