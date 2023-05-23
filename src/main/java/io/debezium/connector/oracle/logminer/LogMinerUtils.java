package io.debezium.connector.oracle.logminer;

import com.alibaba.fastjson.JSON;

import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class LogMinerUtils {

    public static Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

    public static String logEventInfo(List<LogEvent> logEvents) {
        if (logEvents == null || logEvents.size() == 0) {
            return "empty log events";
        }

        StringBuilder sbr = new StringBuilder();
        LogEvent firstLogEvent = logEvents.get(0);

        sbr.append("log event size:").append(logEvents.size()).append(",");
        sbr.append("first log event info:").append("operation code:")
                .append(firstLogEvent.getOperationCode()).append(", scn:").append(firstLogEvent.getScn())
                .append(", changeTime:").append(firstLogEvent.getChangeTime());
        if (logEvents.size() > 1) {
            LogEvent lastLogEvent = logEvents.get(logEvents.size() - 1);
            sbr.append(",last log event info:").append("operation code:")
                    .append(lastLogEvent.getOperationCode()).append(", scn:").append(lastLogEvent.getScn())
                    .append(", changeTime:").append(lastLogEvent.getChangeTime());
        }
        return sbr.toString();
    }
}
