package io.debezium.connector.oracle.logminer;

import com.asw.mdm.stream.etl.utils.NamingThreadFactory;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.*;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.setNlsSessionParameters;

@Slf4j
public class SplitLogFetcherManager {

    public static Duration resetConnectionTimeout = Duration.ofMinutes(10);

    private ExecutorService splitLogFetchPool;
    private BlockingQueue<ScnSplitInfo> pendingLogMinerQueue;
    LogProcessorContext logProcessorContext;
    Configuration jdbcConfig;

    private JdbcConfiguration jdbcConfiguration;
    private OracleConnectorConfig.LogMiningStrategy strategy;
    private ErrorHandler errorHandler;
    OracleConnectorConfig connectorConfig;
    OracleConnection jdbcConnection;
    private boolean isContinuousMining;
    private OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final OracleDatabaseSchema schema;

//    List<OracleConnection> connectionList = new ArrayList<>();

    volatile Throwable logMinerError;

    public int getPendingLogMinerQueueSize() {
        return pendingLogMinerQueue.size();
    }

    public Scn getPendingLogMinerFirstScn() {
        ScnSplitInfo firstSplitInfo = pendingLogMinerQueue.peek();
        if (firstSplitInfo != null) {
            return firstSplitInfo.getStartScn();
        }
        return null;
    }

    public SplitLogFetcherManager(BlockingQueue<ScnSplitInfo> pendingLogMinerQueue, LogProcessorContext logProcessorContext,
                                  OracleConnectorConfig connectorConfig,
                                  OracleConnection jdbcConnection,
                                  ErrorHandler errorHandler,
                                  OracleDatabaseSchema schema,
                                  Configuration jdbcConfig,
                                  OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.pendingLogMinerQueue = pendingLogMinerQueue;
        this.logProcessorContext = logProcessorContext;

        NamingThreadFactory namingThreadFactory = new NamingThreadFactory("split-log-fetch");

        namingThreadFactory.setUncaughtExceptionHandler((t, e) -> {
            logMinerError = e;
        });

//        int parallelNum = logProcessorContext.getLogFetchParallel();
        splitLogFetchPool = new ThreadPoolExecutor(logProcessorContext.getMaxLogFetcherThread(), logProcessorContext.getMaxLogFetcherThread(),
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                namingThreadFactory);

        this.schema = schema;
        this.jdbcConnection = jdbcConnection;
        this.connectorConfig = connectorConfig;
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.streamingMetrics = streamingMetrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
    }

    AtomicInteger fetchThreadCnt = new AtomicInteger(-1);
    Map<Integer, SplitLogFetch> logFetchMap = new ConcurrentHashMap<>();
    public void start() {
        int parallelNum = logProcessorContext.getLogFetchParallel();

        // 全量阶段的 日志读取
        if (logProcessorContext.isSnapshotLogPhase()) {
            parallelNum = 1;
        }

        for (int i = 0; i < parallelNum; i++) {
            incLogFetchThread();
        }
    }

    public void incLogFetchThread() {

        // from [0, maxLogFetcherThread)
        int currentThreadCnt = fetchThreadCnt.get();
        if (currentThreadCnt >= logProcessorContext.getMaxLogFetcherThread() - 1) {
            log.info("the log fetcher thread exceeded the max size, skip. maxFetchThread:{}",
                    logProcessorContext.getMaxLogFetcherThread());
            return;
        }

        Optional<OracleConnection> oracleConnection = newJdbcConnection(jdbcConnection);
        if (oracleConnection.isPresent()) {
            fetchThreadCnt.incrementAndGet();
            SplitLogFetch splitLogFetch = new SplitLogFetch(pendingLogMinerQueue, logProcessorContext,
                    connectorConfig,
                    oracleConnection.get(),
                    errorHandler,
                    schema,
                    jdbcConfig,
                    streamingMetrics,
                    "logMinerFetch-" + fetchThreadCnt.get() + " ");
            splitLogFetchPool.execute(splitLogFetch);

            logFetchMap.put(fetchThreadCnt.get(), splitLogFetch);
            log.info("add new split log fetch thread:{}, size:{}", fetchThreadCnt.get(), logFetchMap.size());
        } else {
            log.error("incLogFetchThread new jdbc connection return null.");
        }

    }

    public void decLogFetchThread() {

        int currentThreadCnt = fetchThreadCnt.get();
        if (currentThreadCnt < logProcessorContext.getLogFetchParallel()) {
            log.info("the log fetcher thread only has the min size thread, skip. minFetcherThread:{}",
                    logProcessorContext.getLogFetchParallel());
            return;
        }

        int fetchThreadNm = fetchThreadCnt.get();

        SplitLogFetch splitLogFetch = logFetchMap.remove(fetchThreadNm);
        if (splitLogFetch != null) {
            fetchThreadCnt.getAndDecrement();
            splitLogFetch.stop();
        }
        log.info("remove split log fetch thread:{}, size:{}", fetchThreadNm, logFetchMap.size());

    }

    public void stop() {
        splitLogFetchPool.shutdown();
        try {
            if (!splitLogFetchPool.awaitTermination(1, TimeUnit.SECONDS)) {
                splitLogFetchPool.shutdownNow(); // Cancel currently executing tasks
            }
        } catch (InterruptedException e) {
            splitLogFetchPool.shutdownNow();
            log.error("shutdown split poll error.", e);
        }

        // split log fetcher close the connection
        /*for (OracleConnection oracleConnection : connectionList) {
            try {
                oracleConnection.close();
            } catch (SQLException e) {
                log.error("close oracle connection error", e);
            }
        }*/
    }

    private static Optional<OracleConnection> newJdbcConnection(OracleConnection jdbcConnection) {

        OracleConnection newJdbcConnection = null;
        try {

            newJdbcConnection = new OracleConnection(jdbcConnection, () -> jdbcConnection.getClass().getClassLoader());
            setNlsSessionParameters(newJdbcConnection);

            return Optional.of(newJdbcConnection);
//            initializeRedoLogsForMining(newJdbcConnection, false, startScn);
        } catch (Exception e) {
            try {
                newJdbcConnection.close();
            } catch (SQLException ex) {
                log.error("newJdbcConnection close jdbc connection error", e);
            }
            log.error("newJdbcConnection error", e);
        }
        return Optional.empty();

    }

    // todo 重新创建连接报错，回来再解决
    /**
     * 2023-01-19 22:09:01,009 ERROR io.debezium.connector.oracle.logminer.SplitLogFetcherManager [] - resetJdbcConnection error
     * java.lang.RuntimeException: Failed to resolve Oracle database version
     * 	at io.debezium.connector.oracle.OracleConnection.resolveOracleDatabaseVersion(OracleConnection.java:164) ~[blob_p-cec2fa25dc5729f8ec87659a26a0436601f67326-4da0dedace1a3d94e6e5e4a531600f64:?]
     * 	at io.debezium.connector.oracle.OracleConnection.<init>(OracleConnection.java:70) ~[blob_p-cec2fa25dc5729f8ec87659a26a0436601f67326-4da0dedace1a3d94e6e5e4a531600f64:?]
     * @return
     */

    /*public static OracleConnection resetJdbcConnection(OracleConnection jdbcConnection) {
        log.info("resetJdbcConnection");
        Optional<OracleConnection> oracleConnection = newJdbcConnection(jdbcConnection);
        if (oracleConnection.isPresent()) {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                log.error("resetJdbcConnection close error", e);
            }
        } else {
            log.error("resetJdbcConnection error");
        }
        return jdbcConnection;
    }*/

    public boolean unRecoverableException() {
        return logMinerError != null;
    }

    public Throwable getLogMinerError() {
        return logMinerError;
    }
}
