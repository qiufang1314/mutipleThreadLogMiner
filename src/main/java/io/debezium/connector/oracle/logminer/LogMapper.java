package io.debezium.connector.oracle.logminer;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class LogMapper {

    private static final int SQL_REDO = 2;
    private static final int CSF = 6;

    public static String getSqlRedo(ResultSet rs)
            throws SQLException {
        int lobLimitCounter = 9; // todo : decide on approach ( XStream chunk option) and Lob limit

        String redoSql = rs.getString(SQL_REDO);
        if (redoSql == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(redoSql);
        int csf = rs.getInt(CSF);

        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        while (csf == 1) {
            rs.next();
            if (lobLimitCounter-- == 0) {
                log.warn("LOB value was truncated due to the connector limitation of {} MB", 40);
                break;
            }

            redoSql = rs.getString(SQL_REDO);
            result.append(redoSql);
            csf = rs.getInt(CSF);
        }

        return result.toString();
    }

}
