/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Array;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.Signal.Payload;
import io.debezium.schema.DataCollectionId;

/**
 * The action to trigger an ad-hoc snapshot.
 * The action parameters are {@code type} of snapshot and list of {@code data-collections} on which the
 * snapshot will be executed.
 *
 * @author Jiri Pechanec
 *
 */
public class ExecuteSnapshot implements Signal.Action {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteSnapshot.class);
    private static final String FIELD_DATA_COLLECTIONS = "data-collections";
    protected static final String FIELD_ADDITIONAL_CONDITION = "additional-condition";
    private static final String FIELD_TYPE = "type";

    public static final String NAME = "execute-snapshot";

    public enum SnapshotType {
        INCREMENTAL
    }

    private final EventDispatcher<? extends DataCollectionId> dispatcher;

    public ExecuteSnapshot(EventDispatcher<? extends DataCollectionId> dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public boolean arrived(Payload signalPayload) throws InterruptedException {
        final Array dataCollectionsArray = signalPayload.data.getArray("data-collections");
        if (dataCollectionsArray == null || dataCollectionsArray.isEmpty()) {
            LOGGER.warn(
                    "Execute snapshot signal '{}' has arrived but the requested field '{}' is missing from data or is empty",
                    signalPayload, FIELD_DATA_COLLECTIONS);
            return false;
        }
        final List<String> dataCollections = dataCollectionsArray.streamValues().map(v -> v.asString().trim())
                .collect(Collectors.toList());
        final String typeStr = signalPayload.data.getString(FIELD_TYPE);
        SnapshotType type = SnapshotType.INCREMENTAL;
        if (typeStr != null) {
            type = SnapshotType.valueOf(typeStr);
        }
        //todo 获取sql条件
        Optional<String> additionalCondition = getAdditionalCondition(signalPayload.data);
        LOGGER.info("Requested '{}' snapshot of data collections '{}' with additional condition '{}'", type, dataCollections,
                additionalCondition.orElse("No condition passed"));
//        LOGGER.info("Requested '{}' snapshot of data collections '{}'", type, dataCollections);
        switch (type) {
            case INCREMENTAL:
//                dispatcher.getIncrementalSnapshotChangeEventSource().addDataCollectionNamesToSnapshot(
//                        dataCollections, signalPayload.offsetContext);
                //todo cj
                dispatcher.getIncrementalSnapshotChangeEventSource().addDataCollectionNamesToSnapshot(
                        dataCollections,additionalCondition, signalPayload.offsetContext,signalPayload.rawData);
                break;
        }
        return true;
    }

    //todo cj 条件需要进一步解析
    public static Optional<String> getAdditionalCondition(Document data) {
        //todo jace 解析内容
        String tmp = data.getField(FIELD_ADDITIONAL_CONDITION).getValue().toString();
        tmp = tmp.replace("{", "").replace("}", "").replace(":", "=");
        String[] split = tmp.split(",");
        ArrayList<String> splitArray = new ArrayList<>();
        for (int i=0; i< split.length;i++){
            String[] split1 = split[i].split("=");
            splitArray.addAll(Arrays.asList(split1));
        }
        StringBuffer additionalConditionBuffer = new StringBuffer();
        for (int i=0; i< splitArray.size();i++){
            if (i % 2 == 0) {
                additionalConditionBuffer.append(splitArray.get(i).replace("\"", "")).append("=");
            } else {
                additionalConditionBuffer.append(splitArray.get(i).replace("\"", "'"));
                if (i != splitArray.size() - 1) {
                    additionalConditionBuffer.append(" and ");
                }
            }

        }
        String additionalCondition = additionalConditionBuffer.toString();
        return (additionalCondition == null || additionalCondition.trim().isEmpty()) ? Optional.empty() : Optional.of(additionalCondition);
    }

}
