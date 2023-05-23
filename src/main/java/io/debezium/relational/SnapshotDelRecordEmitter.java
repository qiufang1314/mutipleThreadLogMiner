package io.debezium.relational;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

public class SnapshotDelRecordEmitter extends RelationalChangeRecordEmitter {
    private final Object[] row;

    public SnapshotDelRecordEmitter(OffsetContext offset, Object[] row, Clock clock) {
        super(offset, clock);

        this.row = row;
    }

    @Override
    protected Operation getOperation() {
        return Operation.DELETE;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return row;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return row;
    }
}
