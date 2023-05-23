package io.debezium.connector.oracle.logminer;

import com.asw.mdm.stream.etl.utils.NamingThreadFactory;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Test implements Runnable {

    AtomicInteger fetchThreadCnt = new AtomicInteger(-1);
    Map<Integer, Integer> logFetchMap = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor dynamicAdjustThreadPool;

    Random rnd = new Random(10);

    public Test() {
        NamingThreadFactory namingThreadFactory = new NamingThreadFactory("dynamic-adjust-log-fetcher");
        dynamicAdjustThreadPool = new ScheduledThreadPoolExecutor(1, namingThreadFactory);
    }

    public static void main(String[] args) {
        Test t = new Test();
        t.dynamicAdjustThreadPool.schedule(t::run, 1, TimeUnit.MILLISECONDS);
    }

    public void incLogFetchThread() {

        // from [0, maxLogFetcherThread)
        int currentThreadCnt = fetchThreadCnt.get();
        if (currentThreadCnt >= 10 - 1) {
            log.info("the log fetcher thread exceeded the max size, skip. maxFetchThread:{}",
                    20);
            return;
        }


        fetchThreadCnt.incrementAndGet();
        logFetchMap.put(fetchThreadCnt.get(), fetchThreadCnt.get());
        log.info("add new split log fetch thread-{}, size:{}", fetchThreadCnt.get(), logFetchMap.size());

    }

    public void decLogFetchThread() {

        int currentThreadCnt = fetchThreadCnt.get();
        if (currentThreadCnt < 4) {
            log.info("the log fetcher thread only has the min size thread, skip. minFetcherThread:{}",
                    4);
            return;
        }

        int fetchThreadNm = fetchThreadCnt.getAndDecrement();
        Integer num = logFetchMap.remove(fetchThreadNm);
        log.info("remove split log fetch thread-{}, size:{}", fetchThreadNm, logFetchMap.size());

    }

    public void run() {

        int rndNum = rnd.nextInt(10);
        // 全量阶段的日志读取
        if (rndNum < 5) {
            incLogFetchThread();
        } else {
            decLogFetchThread();
        }

        dynamicAdjustThreadPool.schedule(this::run, 1, TimeUnit.MILLISECONDS);


    }
}
