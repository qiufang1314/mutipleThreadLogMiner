package io.debezium.connector.oracle.logminer;

import com.asw.mdm.stream.etl.utils.DateUtils;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.asw.mdm.stream.etl.utils.DateUtils.DATE_TIME_FORMAT;

/**
 * (start scn, end scn], archive log [first scn, next scn)
 */
@Slf4j
public class ScnSplitAssign implements Runnable {

    private volatile Scn startScn;
    private Scn bachSizeScn;
    OracleConnection jdbcConnection;
    private Duration period = Duration.ofSeconds(10);

    /**
     * 指定maxSize的阻塞队列
     */
    private BlockingQueue splitQueue;

    private ExecutorService spitPool;

    LogProcessorContext logProcessorContext;

    /**
     * end scn 处于redolog中，不再派发分片信息，直到这个解析完成
     * 针对这种情况的解析，end scn是会 一直往前累加的，下一次的start scn需要注意修改
     */
    private AtomicBoolean endScnIsInRedoLog;

    public ScnSplitAssign(LogProcessorContext logProcessorContext,
                          OracleConnection jdbcConnection,
                          BlockingQueue splitQueue, Scn startScn) {

        this.startScn = startScn;
        this.bachSizeScn = Scn.valueOf(logProcessorContext.getScnBatchRange());
        this.splitQueue = splitQueue;
        this.logProcessorContext = logProcessorContext;
        this.jdbcConnection = jdbcConnection;

        spitPool = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

    }

    public void start() {
        log.info("start scn split thread.");
        spitPool.submit(this::run);
    }

    public void stop() {
        log.info("stop scn split thread start, the queue {}.", splitQueue.size());
        spitPool.shutdown();
        try {
            if (!spitPool.awaitTermination(1, TimeUnit.SECONDS)) {
                spitPool.shutdownNow(); // Cancel currently executing tasks
            }
        } catch (InterruptedException e) {
            spitPool.shutdownNow();
            log.error("shutdown split poll error.", e);
        }
        log.info("stop scn split thread end, the queue {}.", splitQueue.size());
    }


    public void run() {

        log.info("split log assign start run. running:{}", logProcessorContext.isRunning());
        while (logProcessorContext.isRunning()) {
            try {
                Optional<ScnSplitInfo> nexSplit = getNexSplit();
                if (nexSplit.isPresent()) {
                    ScnSplitInfo splitInfo = nexSplit.get();
                    splitQueue.put(splitInfo);
                    log.info("split assign, add split, splitQueueSize:{}, splitInfo:{}", splitQueue.size(), splitInfo);
                    if (splitInfo.isEndScnInRedoLog()) {
                        log.info("split assign, the end scn is in the redo log, wait until the redo log logminer completed, " +
                                "endScn:{}, splitQueueSize:{}, splitInfo:{}",
                                splitInfo.getEndScn(), splitQueue.size(), splitInfo);
                        // 等待 redo解析完成，才继续分片
                        splitInfo.awaitEndScnRedoLogCountDown();
                    }

                    // process start scn
                    startScn = splitInfo.getEndScn();

                } else {
                    Metronome.sleeper(period, Clock.SYSTEM).pause();
                }

            } catch (SQLException e) {
                log.error("get next scn spit sql error", e);
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                log.error("get next scn spit interrupted error", e);
                /*throw new RuntimeException(e);*/
            } catch (Exception e) {
                log.error("get next scn spit error", e);
                throw new RuntimeException(e);
            }
        }

        log.info("split log assign end run. running:{}", logProcessorContext.isRunning());

    }

    /**
     *
     * @return
     * @throws SQLException
     */
    private Optional<ScnSplitInfo> getNexSplit() throws SQLException {

        Scn endScn = startScn.add(bachSizeScn);
        Scn currentScn = jdbcConnection.getCurrentScn();

        // 没有多少日志的
        if (startScn.longValue() >= currentScn.longValue()) {
            return Optional.empty();
        }
        ScnSplitInfo splitInfo = new ScnSplitInfo(logProcessorContext);
        splitInfo.setStartScn(startScn);

        if (endScn.longValue() > currentScn.longValue()) {
            log.info("the calculated end scn is gt current scn, use the current scn as end scn. calculated scn:{}, current scn:{}",
                    endScn, currentScn);
            endScn = currentScn;
        }

        // end scn 在当前真正写到redolog，不需要太多log miner去解析
        if (LogMinerRepository.isScnInCurrentRedoLog(jdbcConnection, endScn)) {
            /*if (splitQueue.size() > logProcessorContext.getEndScnInCurrentRedoLogNum()) {

                log.info("the calculated end scn is in current redo log, we need limit redo logminer parse. endScn:{}",
                        endScn);
                return Optional.empty();
            }*/

            // redo log 适当调大些 end scn
//            endScn = startScn.add(redoLogBachSizeScn);
            // 简单些，不需再判断 start scn是否在archivelog中
            splitInfo.setEndScnInRedoLog(true);
            splitInfo.setEndScn(endScn);

            return Optional.of(splitInfo);
        }

        ArchiveLogPO startPlusOneScnArchiveLogPO = LogMinerRepository.seekArchiveLog(jdbcConnection, startScn.add(Scn.valueOf(1)));
        if (startPlusOneScnArchiveLogPO != null) {
            long archiveLogEndScn = startPlusOneScnArchiveLogPO.getEndScn();
            Scn toEndScn = Scn.valueOf(archiveLogEndScn).subtract(Scn.valueOf(1));

            // 设置小些 endScn，不希望跨多个日志文件
            if ((endScn.compareTo(toEndScn) > 0 && toEndScn.compareTo(startScn) > 0)) {
                log.info("the end scn is bigger then scn in the start scn archive log, use next scn -1 in the archive log, startScn:{}, endScn:{}, toEndScn:{}",
                        startScn, endScn, toEndScn);
                endScn = toEndScn;

            }

            // 即将过期
            if (willExpired(startPlusOneScnArchiveLogPO) && toEndScn.compareTo(startScn) > 0) {
                log.info("will expire soon in the start scn archive log, use next scn - 1 in the archive log, startScn:{}, endScn:{}, toEndScn:{}",
                        startScn, endScn, toEndScn);
                endScn = toEndScn;
            }

        }

        // log miner: start scn, end scn不是从头开始解析文件，，应该是有 根据 scn 能够直接定位 block
       /* int currentArchiveLogNum = LogMinerRepository.countArchiveLogs(jdbcConnection, startScn.add(Scn.valueOf(1)));

        if (currentArchiveLogNum == 1) {
            // 仅有一个日志文件，并发解析这个日志文件
            // archive log: [first scn, next scn); (start scn, end scn]
            ArchiveLogPO startPlusOneScnArchiveLogPO = LogMinerRepository.seekArchiveLog(jdbcConnection, startScn.add(Scn.valueOf(1)));
            if (startPlusOneScnArchiveLogPO != null) {
                long archiveLogEndScn = startPlusOneScnArchiveLogPO.getEndScn();
                Scn toEndScn = Scn.valueOf(archiveLogEndScn).subtract(Scn.valueOf(1));

                // 设置小些 endScn
                if ((endScn.compareTo(toEndScn) > 0 && toEndScn.compareTo(startScn) > 0)) {
                    endScn = toEndScn;
                }

                // 即将过期
                if (willExpired(startPlusOneScnArchiveLogPO) && toEndScn.compareTo(startScn) > 0) {
                    endScn = toEndScn;
                }

                log.info("scn split assign, startScn:{}, endScn:{}, startPlusOneScnArchiveLogPO:{}",
                        startScn, endScn, startPlusOneScnArchiveLogPO);

            }
        } else if (currentArchiveLogNum > 1){
            // 已经存在多个日志文件，
            // archive log: [first scn, next scn); (start scn, end scn]
            ArchiveLogPO startPlusOneScnArchiveLogPO = LogMinerRepository.seekArchiveLog(jdbcConnection, startScn.add(Scn.valueOf(1)));
            // [first scn, next scn)
            long archiveLogEndScn = startPlusOneScnArchiveLogPO.getEndScn();
            // 目的: 并发的解析多个文件
            // 取next scn - 1，不希望跨日志文件
            Scn toEndScn = Scn.valueOf(archiveLogEndScn).subtract(Scn.valueOf(1));
            if (toEndScn.compareTo(startScn) > 0) {
                endScn = toEndScn;
            }

        }*/

        if (endScn.compareTo(startScn) <= 0) {
            log.info("end scn <= start scn, the right range is (start scn, end scn], pause 1s. start scn:{}", startScn);

            try {
                Metronome.sleeper(period, Clock.SYSTEM).pause();
                return getNexSplit();
            } catch (InterruptedException e) {
                log.error("getNextScn sleeper error", e);
            }
        }

        boolean scnInArchiveLog = LogMinerRepository.isScnInArchiveLog(jdbcConnection, endScn);

        splitInfo.setEndScn(endScn);
        splitInfo.setEndScnInArchiveLog(scnInArchiveLog);

        return Optional.of(splitInfo);
    }

    private boolean willExpired(ArchiveLogPO archiveLogPO) {
        try {
            String completionTimeStr = archiveLogPO.getCompletionTime();
            Date completionTime = DateUtils.toDate(completionTimeStr, DATE_TIME_FORMAT);

            Duration archiveLogExpiredThreshold = logProcessorContext.getArchiveLogExpiredThreshold();

            Instant startTime = Instant.ofEpochMilli(completionTime.getTime());

            Duration startToNowDuration = Duration.between(startTime, Instant.now());
            if (startToNowDuration.compareTo(archiveLogExpiredThreshold) >= 0) {
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("willExpired error", e);
            return true;
        }

    }

}
