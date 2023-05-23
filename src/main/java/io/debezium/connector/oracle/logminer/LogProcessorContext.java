package io.debezium.connector.oracle.logminer;

import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Duration;

@Data
public class LogProcessorContext {

    ChangeEventSource.ChangeEventSourceContext context;

    /**
     * scn range 派发的最大个数
     */
    private int scnSplitQueueCapacity;
    private int pendingLogMinerQueueCapacity;
    private int seqLogQueueCapacity;
    /**
     * scn range 大小，值也会根据当前大scn值做判断
     */
    private int scnBatchRange;

    /**
     * 顺序获取大日志保存的 批次，防止内存溢出
     */
    /*private int seqLogParallel;*/

    /**
     * log miner 获取并发度
     */
    private int logFetchParallel;

    /**
     * 计算的end scn 小于 current scn的个数
     */
    private int endScnLtCurrentScnNum;


    /**
     * 获取解析后的事件，间隔
     */
    private Duration logEventPollInterval = Duration.ofMinutes(1);

    /**
     * archive log batch log size,  小于这个值，直接返回这个日志文件的 scn，
     */
//    private BigDecimal archiveBatchLogSize = new BigDecimal("100");

    /**
     * 一次获取log 大小
     */
    private int logPollBatchSize = 1000;

    /**
     * log fetch queue 大小
     */
    private int splitLogFetchResultQueueSize = 16384;

    private Duration archiveLogExpiredThreshold = Duration.ofMinutes(20);

    /**
     * 没有获取到数据，休眠时间
     */
    Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.SYSTEM);

    /**
     * 持续logminer，休眠时间
     */
    private long pauseBetweenContinueLogMiner = 500L;
    private int pauseBetweenContinueLogMinerMultiplier = 4;

    /**
     * 动态调整log fetcher 并发
     */
    private long dynamicAdjustLogFetcherInterval = 5000L;

    private int maxLogFetcherThread = 9;

    /**
     * block size mb
     */
    private BigDecimal minLogSize = new BigDecimal("1000");

    private volatile boolean running;

    public void start() {
        running = true;
    }

    public void stop() {
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isSnapshotLogPhase() {
        if (context instanceof OracleScanFetchTask.SnapshotBinlogSplitChangeEventSourceContext) {
            return true;
        }
        return false;
    }

}
