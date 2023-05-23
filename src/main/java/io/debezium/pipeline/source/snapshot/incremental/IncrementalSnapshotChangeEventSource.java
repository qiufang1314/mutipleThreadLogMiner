/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import io.debezium.document.Document;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

import java.util.List;
import java.util.Optional;

/**
 * A Contract t
 * 
 * @author Jiri Pechanec
 *
 * @param <T> data collection id class
 */
public interface IncrementalSnapshotChangeEventSource<T extends DataCollectionId> {

    void closeWindow(String id, EventDispatcher<T> dispatcher, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext);

    void init(OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext)
            throws InterruptedException;

    void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> additionalCondition, OffsetContext offsetContext)
            throws InterruptedException;

    void addDataCollectionNamesToSnapshot(List<String> dataCollections, Optional<String> additionalCondition, OffsetContext offsetContext, String data)
            throws InterruptedException;
}