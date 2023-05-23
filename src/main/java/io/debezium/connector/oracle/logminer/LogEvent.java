package io.debezium.connector.oracle.logminer;


import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class LogEvent {

    String tableName;
    String segOwner;
    int operationCode;
    Timestamp changeTime;
    String txId;
    String operation;
    String userName;
    String rowId;
    int rollbackFlag;
    Object rsId;
    String redoSql;
    Scn scn;
    TableId tableId;
}
