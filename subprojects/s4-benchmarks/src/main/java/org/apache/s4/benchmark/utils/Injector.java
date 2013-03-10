/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.benchmark.utils;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.RemoteStream;
import org.apache.s4.core.adapter.AdapterApp;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class Injector extends AdapterApp {

    private static Logger logger = LoggerFactory.getLogger(Injector.class);

    @Inject
    @Named("s4.benchmark.testIterations")
    long testIterations;

    @Inject
    @Named("s4.benchmark.keysCount")
    int keysCount;

    @Inject
    @Named("s4.benchmark.pauseTimeMs")
    int pauseTimeMs;

    @Inject
    @Named("s4.benchmark.injector.iterationsBeforePause")
    int iterationsBeforePause;

    @Inject
    @Named("s4.cluster.zk_address")
    String zkString;

    @Inject
    @Named("s4.benchmark.injector.parallelism")
    int parallelism;

    static int CSV_REPORTER_INTERVAL_S = 5;

    // Meter meter = Metrics.newMeter(Injector.class, "injector", "injected", TimeUnit.SECONDS);

    static AtomicLong counter = new AtomicLong();
    static AtomicLong eventCountPerInterval = new AtomicLong();
    BigDecimal rate;
    volatile long lastTime = -1;

    @Override
    protected void onInit() {

        File logDir = new File(System.getProperty("user.dir") + "/measurements/injectors/"
                + getReceiver().getPartitionId());
        if (!logDir.mkdirs()) {
            logger.debug("Cannot create dir " + logDir.getAbsolutePath());
        }
        CsvReporter.enable(logDir, CSV_REPORTER_INTERVAL_S, TimeUnit.SECONDS);
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

        ZooKeeper zk;
        try {
            zk = new ZooKeeper(zkString, 10000, null);

            // NOTE: processing nodes cluster name is hardcoded!
            int nbProcessingNodes = zk.getChildren("/s4/clusters/testCluster2/tasks", null).size();
            CountDownLatch signalNodesConnected = new CountDownLatch(1);
            Utils.watchAndSignalChildrenReachedCount("/s4/streams/" + getRemoteStream().getName() + "/consumers",
                    signalNodesConnected, zk, nbProcessingNodes);
            logger.info("Waiting for all consumers for stream {}", "inputStream");
            signalNodesConnected.await();
            logger.info("All consumers reached for stream {}, proceeding to injection", getRemoteStream().getName());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Cannot fetch config info from zookeeper", e);
            System.exit(1);
        }

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

        generateEvents(remoteStream, testIterations, keysCount, parallelism);

        try {
            // make sure the last log is written
            Thread.sleep(CSV_REPORTER_INTERVAL_S * 1000 + 5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        logger.info("Tests completed after {} test events sent to stream {}", testIterations * parallelism * keysCount,
                getRemoteStream().getName());

        System.exit(0);
    }

    private void generateEvents(RemoteStream remoteStream, long iterations, int keysCount, int parallelism) {

        ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);
        for (int i = 0; i < parallelism; i++) {
            threadPool.submit(new InjectionTask(iterations, remoteStream));
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class InjectionTask implements Runnable {

        private final long iterations;
        private final RemoteStream remoteStream;

        public InjectionTask(long iterations, RemoteStream remoteStream) {
            super();
            this.iterations = iterations;
            this.remoteStream = remoteStream;

        }

        @Override
        public void run() {
            for (long i = 0; i < iterations; i++) {
                for (int j = 0; j < keysCount; j++) {
                    long currentCount = counter.incrementAndGet();
                    Event event = new Event();
                    event.put("key", int.class, j);
                    event.put("value", long.class, currentCount);
                    event.put("injector", Integer.class, getReceiver().getPartitionId());
                    // logger.info("{}/{}/{}/",
                    // new String[] { Thread.currentThread().getName(), String.valueOf(i), String.valueOf(j),
                    // String.valueOf(event.get("value")) });
                    remoteStream.put(event);
                    eventCountPerInterval.incrementAndGet();

                    try {
                        if ((currentCount % iterationsBeforePause) == 0) {
                            Thread.sleep(pauseTimeMs);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }
}
