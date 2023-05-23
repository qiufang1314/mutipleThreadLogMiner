package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.Scn;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

@Data
public class LogEventBatch {

    /**
     * split info
     */
    private Scn startScn;
    private Scn endScn;

    private List<LogEvent> logEvents;

    /**
     * 这一批次已经完成
     */
    private boolean splitScnFetchFinished;

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(logEvents);
    }

    @Override
    public String toString() {
        return "LogEventBatch{" +
                "startScn=" + startScn +
                ", endScn=" + endScn +
                ", splitScnFetchFinished=" + splitScnFetchFinished +
                ", logEvents=" + LogMinerUtils.logEventInfo(logEvents) +
                '}';
    }
}
