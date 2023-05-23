package io.debezium.connector.oracle.logminer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.asw.mdm.stream.etl.utils.NamingThreadFactory;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class DynamicAdjustLogFetcherManager implements Runnable {

    private LogProcessorContext logProcessorContext;
    OracleConnection jdbcConnection;
    private SplitLogFetcherManager splitLogFetcherManager;

    private ScheduledThreadPoolExecutor dynamicAdjustThreadPool;

    private volatile boolean isRunning;

    public DynamicAdjustLogFetcherManager(LogProcessorContext logProcessorContext,
                                          SplitLogFetcherManager splitLogFetcherManager,
                                          OracleConnection jdbcConnection
                                          ) {
        this.logProcessorContext = logProcessorContext;
        this.jdbcConnection = jdbcConnection;
        this.splitLogFetcherManager = splitLogFetcherManager;

        NamingThreadFactory namingThreadFactory = new NamingThreadFactory("dynamic-adjust-log-fetcher");
        dynamicAdjustThreadPool = new ScheduledThreadPoolExecutor(1, namingThreadFactory);
    }

    public void start() {
        log.info("start DynamicAdjustLogFetcherManager thread.");
        isRunning = true;
        dynamicAdjustThreadPool.schedule(this::run, logProcessorContext.getDynamicAdjustLogFetcherInterval(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        log.info("stop DynamicAdjustLogFetcherManager");
        isRunning = false;
        dynamicAdjustThreadPool.shutdown();
        try {
            if (!dynamicAdjustThreadPool.awaitTermination(1, TimeUnit.SECONDS)) {
                dynamicAdjustThreadPool.shutdownNow(); // Cancel currently executing tasks
            }
        } catch (InterruptedException e) {
            dynamicAdjustThreadPool.shutdownNow();
            log.error("shutdown split poll error.", e);
        }
    }


    @Override
    public void run() {

        // 全量阶段的日志读取
        if (logProcessorContext.isSnapshotLogPhase()) {
            return;
        }

        try {

            Scn firstScn = splitLogFetcherManager.getPendingLogMinerFirstScn();
            if (firstScn != null) {
                Object timestamp = LogMinerRepository.scnTime(jdbcConnection, firstScn);
                log.info("the scn {} in the first pending logminer queue utc timestamp is {}, scnTimePrint: {}", firstScn, timestamp,
                        scnTimePrint(timestamp));
            }

            int pendingLogMinerQueueSize = splitLogFetcherManager.getPendingLogMinerQueueSize();
            if (pendingLogMinerQueueSize > 2) {
                log.info("the pending logminer size:{} > 2 , increment the fetch thread count.",
                        pendingLogMinerQueueSize);
                splitLogFetcherManager.incLogFetchThread();
            } else {
                if (firstScn != null) {
                    AchvieLogCountVO achvieLogCountVO = LogMinerRepository.logSize(jdbcConnection, firstScn.add(Scn.valueOf(1)));
                    if (achvieLogCountVO != null) {
                        BigDecimal logSize = achvieLogCountVO.getLogSize();
                        int logCnt = achvieLogCountVO.getLogCnt();

                        // adjust fetcher thread
                        if (logCnt > 1 ||
                                logSize.compareTo(logProcessorContext.getMinLogSize()) >= 0) {
                            log.info("log count size > 1 || logSize > {}, increment the fetch thread count. logCnt:{}, logSize:{}, firstScn:{}",
                                    logProcessorContext.getMinLogSize(), logCnt, logSize, firstScn);
                            splitLogFetcherManager.incLogFetchThread();
                        } else {
                            log.info("log count size <= 1 || logSize <= {}, decrement the fetch thread count. logCnt:{}, logSize:{}, firstScn:{}",
                                    logProcessorContext.getMinLogSize(), logCnt, logSize, firstScn);
                            splitLogFetcherManager.decLogFetchThread();
                        }

                    } else {
                        log.info("has no archive log, decrement the fetch thread count.");
                        splitLogFetcherManager.decLogFetchThread();
                    }
                }
            }


        } catch (Exception e) {
            log.info("dynamic adjust log fetcher thead error.", e);
        }

        if (logProcessorContext.isRunning() && isRunning) {
            dynamicAdjustThreadPool.schedule(this::run, logProcessorContext.getDynamicAdjustLogFetcherInterval(), TimeUnit.MILLISECONDS);
        } else {
            log.info("break dynamic adjust log fetcher thread");
        }
    }

    /**
     * 方便日志中搜索
     * @param timestamp
     * @return
     */
    private static String scnTimePrint(Object timestamp) {

        try {
            StringBuilder sbr = new StringBuilder();
            Map scnTimeMap = new HashMap();
            scnTimeMap.put("scnTime", timestamp);

            String scnMapStr = JSON.toJSONString(scnTimeMap);
            JSONObject jsonObject = JSONObject.parseObject(scnMapStr);

            Timestamp scnTime = jsonObject.getTimestamp("scnTime");
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());

            long diffTime = scnTime.getTime() - currentTime.getTime();
            long diffLogRangeMin = diffTime / LOG_RANGE_MIN;

            sbr.append("scn time is multiple:").append(diffLogRangeMin).append(" logRangeMin:5");

            return sbr.toString();
        } catch (Exception e) {
            log.error("scnTimePrint error", e);
        }

        return "";
    }

    private static long ONE_MIN_MS = 60 * 1000;
    private static long LOG_RANGE_MIN = 5 * ONE_MIN_MS;


    public static void main(String[] args) {
        Date date = new Date();
        Date addDate = DateUtils.addHours(date, 1);

        long diffTime = date.getTime() - addDate.getTime();

        long diffLogRangeMin = diffTime / LOG_RANGE_MIN;

        log.info(" scn time is multiple:{} logRangeMin:{}  ", diffLogRangeMin, 5);
//        scnTimePrint(date);
    }
}
