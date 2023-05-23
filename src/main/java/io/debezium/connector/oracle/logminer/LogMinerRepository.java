package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;


@Slf4j
public class LogMinerRepository {


    /**
     * select *
     * from v$archived_log
     * where first_change# <= 13538234400775 and next_change# > 13538234400775;
     *
     * @param scn
     * @return
     */
    public static boolean isScnInArchiveLog(OracleConnection jdbcConnection, Scn scn) {
        String sql = "select RECID \n" +
                " from v$archived_log A \n" +
                " where first_change# <= ? and next_change# > ? AND A.ARCHIVED = 'YES' AND A.STATUS = 'A' ";

        ResultSet rs = null;
        try (PreparedStatement logStatement = jdbcConnection.connection().prepareStatement(sql)) {

            logStatement.setString(1, scn.toString());
            logStatement.setString(2, scn.toString());
            rs = logStatement.executeQuery();
            boolean hasNext = rs.next();
            rs.close();
//            log.info("Is scn in archive log?, scn:{}, return:{}", scn, hasNext);
            return hasNext;

        } catch (Exception e) {
            log.error("isScnInArchiveLog error", e);
            return false;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }

    }

    public static boolean isScnInCurrentRedoLog(OracleConnection jdbcConnection, Scn endScn) throws SQLException {
        String sql = "select *\n" +
                "from v$log where STATUS = 'CURRENT'\n" +
                " and first_change# <= " +
                endScn.longValue() +
                " and next_change# > " +
                endScn.longValue();

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                return true;
            }
            return false;
        });
    }

    //判断scn 是否在logfile中，目前暂时未找到更合适的sql
    public static boolean isScnInlogfile(OracleConnection jdbcConnection, Scn startScn,boolean isContinuousMining) throws SQLException {

        String miningStrategy = "";
        if (isContinuousMining) {
            miningStrategy += " + DBMS_LOGMNR.CONTINUOUS_MINE ";
        }
        String sql = "BEGIN DBMS_LOGMNR.START_LOGMNR(" +
                "STARTSCN => " + startScn + ", " +
                "ENDSCN => " + startScn.add(new Scn(new BigInteger("1"))) + ", " +
                " OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + DBMS_LOGMNR.SKIP_CORRUPTION" + miningStrategy+
                ");" +
                "END;";
        try {
            jdbcConnection.execute(sql);
            return true;
        }catch (Exception e){
//            log.error(e.toString());
            return false;
        }

    }

    public static ArrayList<String> getArchiveLogList(OracleConnection jdbcConnection,Long retetionHours) throws SQLException {
        String sql = "SELECT FIRST_CHANGE#\n" +
                "FROM v$archived_log\n" +
                "WHERE SYSDATE - FIRST_time<"+(float)retetionHours/24.0f+"\n"+
                "ORDER BY FIRST_time";

        return jdbcConnection.queryAndMap(sql, rs -> {
            ArrayList<String> arrayList = new ArrayList<>();
            while (rs.next()){
                arrayList.add(rs.getString(1));
            }

            return arrayList;
        });
    }
    public static ArrayList getLogList(OracleConnection jdbcConnection) throws SQLException {
        String sql = "SELECT first_change#\n" +
                "FROM v$log\n" +
                "ORDER BY first_time ";
        return jdbcConnection.queryAndMap(sql, rs -> {
            ArrayList<String> arrayList = new ArrayList<>();
            while (rs.next()){
                arrayList.add(rs.getString(1));
            }

            return arrayList;
        });
    }





    /**
     * @param startScn,
     */
    public static ArchiveLogPO seekArchiveLog(OracleConnection jdbcConnection, Scn startScn) throws SQLException {

        String sql = "select to_char(completion_time, 'yyyy-mm-dd hh24:MI:SS') completion_time, " +
                " name, round(blocks * block_size / 1024 / 1024, 2) as block_size,\n" +
                "       first_change# as start_scn, next_change# as end_scn, next_change# - first_change# as scn_range\n" +
                "from v$archived_log A\n" +
                "where first_change# <= " +
                startScn.longValue() +
                " and next_change# > " +
                startScn.longValue() +
                " AND A.ARCHIVED = 'YES' AND A.STATUS = 'A'";

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                String name = rs.getString("name");
                BigDecimal blockSize = rs.getBigDecimal("block_size");
                String completionTime = rs.getString("completion_time");
                long startScn1 = rs.getLong("start_scn");
                long endScn = rs.getLong("end_scn");
                long scnRange = rs.getLong("scn_range");
                return new ArchiveLogPO(blockSize, startScn1, endScn, scnRange, name, completionTime);
            }
            return null;
        });

    }

    public static int countArchiveLogs(OracleConnection jdbcConnection, Scn startScn) throws SQLException {
        String sql = "select count(1) cnt\n" +
                " from v$archived_log A\n" +
                " where next_change# > " +
                startScn.longValue() +
                " AND A.ARCHIVED = 'YES' AND A.STATUS = 'A'";

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                return rs.getInt("cnt");

            }
            return 0;
        });

    }

    public static AchvieLogCountVO logSize(OracleConnection jdbcConnection, Scn startScn) throws SQLException {
        String sql = "select nvl(sum(round(blocks * block_size / 1024 / 1024, 2)),0) as block_size, count(*) cnt \n" +
                "from v$archived_log A\n" +
                "where next_change# > " +
                startScn.longValue() +
                " AND A.ARCHIVED = 'YES' AND A.STATUS = 'A'";

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                BigDecimal logSize = rs.getBigDecimal("block_size");
                int cnt = rs.getInt("cnt");
                return new AchvieLogCountVO(logSize, cnt);
            }
            return null;
        });
    }

    public static Object scnTime(OracleConnection jdbcConnection, Scn startScn) throws SQLException {
        String sql = "select scn_to_timestamp(" +
                startScn.longValue() +
                ") scn_time from dual";

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                return rs.getObject("scn_time");
            }
            return null;
        });
    }
    public static String getCurrentScn(JdbcConnection jdbcConnection) throws SQLException {

        return jdbcConnection.queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return rs.getString(1);
            }
            throw new IllegalStateException("Could not get Current SCN");
        });
    }

    public static String getDBTimeZone(JdbcConnection jdbcConnection) throws SQLException {

        return jdbcConnection.queryAndMap("SELECT dbtimezone FROM dual", (rs) -> {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "Asia/Shanghai";
//            throw new IllegalStateException("Could not get dbtimezone");
        });
    }

}
