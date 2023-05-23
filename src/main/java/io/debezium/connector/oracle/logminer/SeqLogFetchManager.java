package io.debezium.connector.oracle.logminer;

import io.debezium.util.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class SeqLogFetchManager implements Runnable {

    BlockingQueue<ScnSplitInfo> splitQueue;

    BlockingQueue<ScnSplitInfo> pendingLogMinerQueue;

    BlockingQueue<ScnSplitInfo> seqLogQueue;

    private ExecutorService splitInfoTransferToLogPool;

    LogProcessorContext logProcessorContext;

    /**
     * 当前待处理的分片数据
     */
    ScnSplitInfo currentSplitInfo;

    public SeqLogFetchManager(BlockingQueue splitQueue, BlockingQueue<ScnSplitInfo> pendingLogMinerQueue,
                              LogProcessorContext logProcessorContext) {
        this.splitQueue = splitQueue;
        this.pendingLogMinerQueue = pendingLogMinerQueue;
        this.logProcessorContext = logProcessorContext;
        this.splitInfoTransferToLogPool = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        seqLogQueue = new ArrayBlockingQueue<>(logProcessorContext.getSeqLogQueueCapacity());
    }

    public void start() {
        log.info("seq log fetch ");
        splitInfoTransferToLogPool.submit(this::run);
    }

    public void stop() {
        log.info("start to seq log stop, the queue size:{}.", splitQueue.size());
        splitInfoTransferToLogPool.shutdown();
        try {
            if (!splitInfoTransferToLogPool.awaitTermination(1, TimeUnit.SECONDS)) {
                splitInfoTransferToLogPool.shutdownNow(); // Cancel currently executing tasks
            }
        } catch (InterruptedException e) {
            splitInfoTransferToLogPool.shutdownNow();
            log.error("shutdown split poll error.", e);
        }
        log.info("stop seq log, the queue size:{}.", splitQueue.size());
    }


    public void run() {

        log.info("seq log fetch, start run. running:{}", logProcessorContext.isRunning());
        while (logProcessorContext.isRunning()) {
            try {
                ScnSplitInfo splitInfo = splitQueue.poll(1, TimeUnit.SECONDS);
                if (splitInfo == null) {
//                    log.info("split info get, get null spit info, try again.");
                    continue;
                }
                log.info("get one split info, splitInfo:{}", splitInfo);
                pendingLogMinerQueue.put(splitInfo);
                // 控制大小，控制顺序
                seqLogQueue.put(splitInfo);

            } catch (InterruptedException e) {
                log.error("seq log fetch interrupted exception", e);
               /* throw new RuntimeException(e);*/
            } catch (Exception e) {
                log.error("seq log fetch exception", e);
                throw new RuntimeException(e);
            }

        }
        log.info("seq log fetch end run");

    }

    /**
     * 获取log miner 解析后都数据，获取到数或者这一批次已经完成 才返回
     * @return
     * @throws InterruptedException
     */
    public LogEventBatch awaitToGetNextBatchLog() throws InterruptedException {
        /*Stopwatch stopwatch = Stopwatch.reusable();
        stopwatch.start();*/

        LogEventBatch logEventBatch = getNextBatchLog();
        long printTime = System.currentTimeMillis() % 3;
        if (printTime==0){
            log.info("seq log get fetched log event, logEventBatch:{}, seqLogQueueSize:{}", logEventBatch, seqLogQueue.size());
        }

        /*log.info("get log miner log events, duration:{}, logEventBatch:{}", stopwatch.stop().durations().statistics().getTotal(),
                logEventBatch);*/

        return logEventBatch;
    }


    private LogEventBatch getNextBatchLog() throws InterruptedException {

        if (currentSplitInfo == null) {
            currentSplitInfo = seqLogQueue.take();
            log.info("seq log is processing the currentSplitInfo:{}, seqLogQueueSize:{}", currentSplitInfo, seqLogQueue.size());
            return getNextBatchLog();
        }

        LogEventBatch logEventBatch = currentSplitInfo.blockBatchLogsUntilTimeout(logProcessorContext);
        if (logEventBatch.isSplitScnFetchFinished()) {
            // 下一批次，需要从队列中获取
            currentSplitInfo = null;

        }

        return logEventBatch;

    }


}
