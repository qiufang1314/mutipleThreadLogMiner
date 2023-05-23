package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.Scn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvailableScnVO {
    private Scn scn;
    private boolean scnInlogfile;

}
