package io.debezium.connector.oracle.logminer;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.oracle.Scn;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.Stopwatch;
import io.debezium.util.Threads;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (startScn, endScn]
 */
@Data
@Slf4j
public class ScnSplitInfo {



    private Scn startScn;
    // 线程可见性，重排
    private volatile Scn endScn;

    /**
     * end scn 在 redo log中，需要特别处理，不然大部分logminer线程都在解析 小范围内的 redo log日志，
     * 而且不能退出，必须要等到redolog被archived，没有多少日志的情况下，延迟会去到10多分钟
     * todo 需要找Ronny确认下，achivelog文件的时间是创建时间还是最后修改时间，日志量小的时候差别有10多分钟
     */
    private boolean isEndScnInRedoLog;

    /**
     * end scn 对应的log文件已经 archive log日志中，可以大胆的使用~
     */
    private boolean isEndScnInArchiveLog;
    private volatile boolean processed;
    private BlockingQueue<LogEvent> logEvents;
    private AtomicInteger rows = new AtomicInteger();

    private volatile Scn lastProcessedScn;
    private volatile LogEvent firstLogEvent;
    private volatile LogEvent lastLogEvent;

  /*  *//**
     * 更新后的endScn
     *//*
    private AtomicReference<Scn> redoLogMinerEndScn;*/

    private Stopwatch costSt = Stopwatch.reusable();

    private LogProcessorContext logProcessorContext;

    private CountDownLatch endScnRedoLogCountDownLatch = new CountDownLatch(1);
    private AtomicBoolean onceEndScnRedoLogCountFlag = new AtomicBoolean(false);

    public void awaitEndScnRedoLogCountDown() throws InterruptedException {
        /*endScnRedoLogCountDownLatch = new CountDownLatch(1);*/
        endScnRedoLogCountDownLatch.await();
    }

    public boolean endScnRedoLogCountDown(Scn endScn) {
        if (onceEndScnRedoLogCountFlag.compareAndSet(false, true)) {
            this.endScn = endScn;
            endScnRedoLogCountDownLatch.countDown();
            log.info("the redo log has completed, splitInfo:{}", this);
            return true;
        }
        return false;
    }

    public ScnSplitInfo(LogProcessorContext logProcessorContext) {
        logEvents = new LinkedBlockingQueue<>(logProcessorContext.getSplitLogFetchResultQueueSize());
        this.logProcessorContext = logProcessorContext;
        costSt.start();
    }

    public void addLogEvent(LogEvent logEvent, String logPrefix) throws InterruptedException {
        if (firstLogEvent == null) {
            firstLogEvent = logEvent;
        }
        lastLogEvent = logEvent;
        rows.incrementAndGet();
        int remainingSize = logProcessorContext.getSplitLogFetchResultQueueSize();
        if (logEvents.size() == remainingSize) {
            log.info(logPrefix +
                            "the log event has reached the max capacity wait. splitInfo:{}, logEventSize:{}",
                    this, remainingSize);
        }
        logEvents.put(logEvent);
    }

    public void finish(String logPrefix) {
        processed = true;
        int remainingSize = logEvents.size();
        log.info(logPrefix +
                        " the split info finished cost:{}, totalRows {} 0, remainingRows:{}, remainingRows {} 0" +
                        delayInfo() +
                        ", splitInfo:{}",
                costSt.stop().durations().statistics().getTotal(),
                rows.get() > 0 ? "gt" : "eq", remainingSize, remainingSize > 0 ? "gt" : "eq", this);
       /* awaitToFinish.countDown();*/
    }

    private String delayInfo() {
        StringBuilder sbr = new StringBuilder();
        if (processed && rows.get() > 0) {
            sbr.append(", delay:");
            // utc time
            Timestamp changeTime = lastLogEvent.getChangeTime();
            long timeInMillis = LogMinerUtils.UTC_CALENDAR.getTimeInMillis();
            long delayTime = timeInMillis - changeTime.getTime();
            sbr.append(delayTime).append("ms,");
            return sbr.toString();

        }
        return "";
    }

    /**
     * todo 代码移到 util类
     * @param processorContext
     * @return
     * @throws InterruptedException
     */
    public LogEventBatch blockBatchLogsUntilTimeout(LogProcessorContext processorContext) throws InterruptedException {

        LogEventBatch logEventBatch = new LogEventBatch();
        logEventBatch.setStartScn(startScn);
        logEventBatch.setEndScn(endScn);

        List<LogEvent> records = new ArrayList<>();
        if (processed) {
            // 处理完成，全部返回
            logEvents.drainTo(records);
            logEventBatch.setLogEvents(records);
            logEventBatch.setSplitScnFetchFinished(true);
            return logEventBatch;
        }

        final Threads.Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.min(processorContext.getLogEventPollInterval(),
                ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
        while (!timeout.expired() && logEvents.drainTo(records, processorContext.getLogPollBatchSize()) == 0) {

            // check again
            if (processed) {
                // 处理完成，全部返回
                logEvents.drainTo(records);
                logEventBatch.setLogEvents(records);
                logEventBatch.setSplitScnFetchFinished(true);
                return logEventBatch;
            }

            // no records yet, so wait a bit
            processorContext.getMetronome().pause();
        }

        logEventBatch.setLogEvents(records);
        logEventBatch.setSplitScnFetchFinished(false);
        return logEventBatch;

    }

   /* public List<LogEvent> awaitLogEvents() throws InterruptedException {
        awaitToFinish.await();
        return  logEvents;
    }*/

    public void resetLogEvents() {
        logEvents = new LinkedBlockingQueue<>();
    }

    public boolean isProcessed() {
        return processed;
    }

    @Override
    public String toString() {
        return "ScnSplitInfo{" +
                "startScn=(" + startScn + ")" +
                ", endScn=[" + endScn + "]" +
                ", remainLogEventsSize=" + logEvents.size() +
                ", processed=" + processed +
                moreFinishedInfo() +
                redoLogInfo() +
                '}';
    }

    private String redoLogInfo() {
        StringBuilder sbr = new StringBuilder();
        sbr
                .append("isEndScnInRedoLog=").append(isEndScnInRedoLog)
                .append(",redoLogMinerEndScn:").append(endScn);

        return sbr.toString();
    }

    private String moreFinishedInfo() {
        if (processed && rows.get() > 0) {
            StringBuilder sbr = new StringBuilder();
            sbr.append(",finishedInfo{")
                    .append("totalRows=").append(rows)
                    .append(",firstLogEventInfo:").append(firstLogEvent)
                    .append(",lastLogScn:").append(lastLogEvent)
                    .append("}");

            return sbr.toString();
        }

        return "";
    }

}
