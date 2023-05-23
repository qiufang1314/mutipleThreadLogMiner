package io.debezium.connector.oracle.logminer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Duration;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ArchiveLogPO {

    /**
     * mb
     */
    BigDecimal blockSize;
    long startScn;
    long endScn;
    long scnRange;
    String name;
    String completionTime;

}
