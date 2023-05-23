package io.debezium.connector.oracle.logminer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AchvieLogCountVO {

    private BigDecimal logSize;
    private int logCnt;
}
