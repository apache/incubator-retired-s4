package org.apache.s4.benchmark.simpleApp1;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.RemoteStream;
import org.apache.s4.core.adapter.AdapterApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.reporting.ConsoleReporter;

public class Injector extends AdapterApp {

    private static Logger logger = LoggerFactory.getLogger(Injector.class);

    @Inject
    @Named("s4.benchmark.warmupIterations")
    long warmupIterations;

    @Inject
    @Named("s4.benchmark.testIterations")
    long testIterations;

    @Inject
    @Named("s4.benchmark.keysCount")
    int keysCount;

    @Inject
    @Named("s4.benchmark.warmupSleepInterval")
    int warmupSleepInterval;

    @Inject
    @Named("s4.benchmark.testSleepInterval")
    int testSleepInterval;

    @Inject
    @Named("s4.cluster.zk_address")
    String zkString;

    @Inject
    @Named("s4.benchmark.injector.parallelism")
    int parallelism;

    // Meter meter = Metrics.newMeter(Injector.class, "injector", "injected", TimeUnit.SECONDS);

    static AtomicLong counter = new AtomicLong();
    static AtomicLong eventCountPerInterval = new AtomicLong();
    BigDecimal rate;
    volatile long lastTime = -1;

    @Override
    protected void onInit() {

        File logDir = new File(System.getProperty("user.dir") + "/measurements/injectors");
        if (!logDir.mkdirs()) {
            logger.debug("Cannot create dir " + logDir.getAbsolutePath());
        }
        // CsvReporter.enable(logDir, 5, TimeUnit.SECONDS);
        remoteStreamKeyFinder = new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(event.get("key"));
            }
        };
        super.onInit();
        ConsoleReporter.enable(30, TimeUnit.SECONDS);
    }

    @Override
    protected void onStart() {

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                if (lastTime == -1) {
                    lastTime = System.currentTimeMillis();
                } else {
                    if ((System.currentTimeMillis() - lastTime) > 1000) {
                        rate = new BigDecimal(eventCountPerInterval.getAndSet(0)).divide(
                                new BigDecimal(System.currentTimeMillis() - lastTime), MathContext.DECIMAL64).multiply(
                                new BigDecimal(1000), MathContext.DECIMAL64);
                        lastTime = System.currentTimeMillis();
                    }
                }

            }
        }, 1, 1, TimeUnit.SECONDS);

        Metrics.newGauge(Injector.class, "injection-rate", new Gauge<BigDecimal>() {

            @Override
            public BigDecimal value() {
                return rate;
            }
        });

        RemoteStream remoteStream = getRemoteStream();

        counter.set(0);

        generateEvents(remoteStream, testIterations, keysCount, testSleepInterval, parallelism);

        logger.info("Tests completed after test events", testIterations * parallelism * keysCount);

        System.exit(0);
    }

    private void generateEvents(RemoteStream remoteStream, long iterations, int keysCount, int sleepInterval,
            int parallelism) {

        ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);
        for (int i = 0; i < parallelism; i++) {
            threadPool.submit(new InjectionTask(iterations, remoteStream, sleepInterval));
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class InjectionTask implements Runnable {

        private long iterations;
        private RemoteStream remoteStream;
        private long sleepInterval;

        public InjectionTask(long iterations, RemoteStream remoteStream, long sleepInterval) {
            super();
            this.iterations = iterations;
            this.remoteStream = remoteStream;
            this.sleepInterval = sleepInterval;

        }

        @Override
        public void run() {
            for (long i = 0; i < iterations; i++) {
                for (int j = 0; j < keysCount; j++) {
                    Event event = new Event();
                    event.put("key", int.class, j);
                    event.put("value", long.class, counter.incrementAndGet());
                    event.put("injector", Integer.class, getReceiver().getPartitionId());
                    // logger.info("{}/{}/{}/",
                    // new String[] { Thread.currentThread().getName(), String.valueOf(i), String.valueOf(j),
                    // String.valueOf(event.get("value")) });
                    remoteStream.put(event);
                    eventCountPerInterval.incrementAndGet();
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
