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
package org.apache.s4.core.util;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.s4.base.Emitter;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.ReceiverImpl;
import org.apache.s4.core.RemoteSender;
import org.apache.s4.core.SenderImpl;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

/**
 * Utility class for centralizing system runtime metrics, such as information about event processing rates, cache
 * eviction etc...
 */
@Singleton
public class S4Metrics {

    private static Logger logger = LoggerFactory.getLogger(S4Metrics.class);

    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv:.+|console):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    @Inject
    Emitter emitter;

    @Inject
    Assignment assignment;

    @Inject(optional = true)
    @Named("s4.metrics.config")
    String metricsConfig;

    static List<Meter> partitionSenderMeters = Lists.newArrayList();

    private final Meter eventMeter = Metrics.newMeter(ReceiverImpl.class, "received-events", "event-count",
            TimeUnit.SECONDS);
    private final Meter bytesMeter = Metrics.newMeter(ReceiverImpl.class, "received-bytes", "bytes-count",
            TimeUnit.SECONDS);

    private final Meter localEventsMeter = Metrics.newMeter(Stream.class, "sent-local", "sent-local", TimeUnit.SECONDS);
    private final Meter remoteEventsMeter = Metrics.newMeter(Stream.class, "sent-remote", "sent-remote",
            TimeUnit.SECONDS);

    private Meter[] senderMeters;

    private final Map<String, Meter> dequeuingStreamMeters = Maps.newHashMap();
    private final Map<String, Meter> droppedStreamMeters = Maps.newHashMap();
    private final Map<String, Meter> streamQueueFullMeters = Maps.newHashMap();
    private final Meter droppedInSenderMeter = Metrics.newMeter(SenderImpl.class, "dropped@sender", "dropped@sender",
            TimeUnit.SECONDS);
    private final Meter droppedInRemoteSenderMeter = Metrics.newMeter(SenderImpl.class, "dropped@remote-sender",
            "dropped@remote-sender", TimeUnit.SECONDS);

    private final Map<String, Meter[]> remoteSenderMeters = Maps.newHashMap();

    @Inject
    private void init() {

        if (Strings.isNullOrEmpty(metricsConfig)) {
            logger.info("Metrics reporting not configured");
        } else {
            Matcher matcher = METRICS_CONFIG_PATTERN.matcher(metricsConfig);
            if (!matcher.matches()) {
                logger.error(
                        "Invalid metrics configuration [{}]. Metrics configuration must match the pattern [{}]. Metrics reporting disabled.",
                        metricsConfig, METRICS_CONFIG_PATTERN);
            } else {
                String group1 = matcher.group(1);

                if (group1.startsWith("csv")) {
                    String outputDir = group1.substring("csv:".length());
                    long period = Long.valueOf(matcher.group(2));
                    TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                    logger.info("Reporting metrics through csv files in directory [{}] with frequency of [{}] [{}]",
                            new String[] { outputDir, String.valueOf(period), timeUnit.name() });
                    CsvReporter.enable(new File(outputDir), period, timeUnit);
                } else {
                    long period = Long.valueOf(matcher.group(2));
                    TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                    logger.info("Reporting metrics on the console with frequency of [{}] [{}]",
                            new String[] { String.valueOf(period), timeUnit.name() });
                    ConsoleReporter.enable(period, timeUnit);
                }
            }
        }

        senderMeters = new Meter[emitter.getPartitionCount()];
        // int localPartitionId = assignment.assignClusterNode().getPartition();
        for (int i = 0; i < senderMeters.length; i++) {
            senderMeters[i] = Metrics.newMeter(SenderImpl.class, "sender", "sent-to-" + (i), TimeUnit.SECONDS);
        }
        Metrics.newGauge(Stream.class, "local-vs-remote", new Gauge<Double>() {
            @Override
            public Double value() {
                // this will return NaN if divider is zero
                return localEventsMeter.oneMinuteRate() / remoteEventsMeter.oneMinuteRate();
            }
        });

    }

    public void createCacheGauges(ProcessingElement prototype, final LoadingCache<String, ProcessingElement> cache) {

        Metrics.newGauge(prototype.getClass(), prototype.getClass().getName() + "-cache-entries", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.size();
            }
        });
        Metrics.newGauge(prototype.getClass(), prototype.getClass().getName() + "-cache-evictions", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.stats().evictionCount();
            }
        });
        Metrics.newGauge(prototype.getClass(), prototype.getClass().getName() + "-cache-misses", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.stats().missCount();
            }
        });
    }

    public void receivedEventFromCommLayer(int bytes) {
        eventMeter.mark();
        bytesMeter.mark(bytes);
    }

    public void queueIsFull(String name) {
        streamQueueFullMeters.get(name).mark();

    }

    public void sentEvent(int partition) {
        remoteEventsMeter.mark();
        try {
            senderMeters[partition].mark();
        } catch (NullPointerException e) {
            logger.warn("Sender meter not ready for partition {}", partition);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Partition {} does not exist", partition);
        }
    }

    public void droppedEventInSender() {
        droppedInSenderMeter.mark();
    }

    public void droppedEventInRemoteSender() {
        droppedInRemoteSenderMeter.mark();
    }

    public void sentLocal() {
        localEventsMeter.mark();
    }

    public void createStreamMeters(String name) {
        // TODO avoid maps to avoid map lookups?
        dequeuingStreamMeters.put(name,
                Metrics.newMeter(Stream.class, "dequeued@" + name, "dequeued", TimeUnit.SECONDS));
        droppedStreamMeters.put(name, Metrics.newMeter(Stream.class, "dropped@" + name, "dropped", TimeUnit.SECONDS));
        streamQueueFullMeters.put(name,
                Metrics.newMeter(Stream.class, "stream-full@" + name, "stream-full", TimeUnit.SECONDS));
    }

    public void dequeuedEvent(String name) {
        dequeuingStreamMeters.get(name).mark();
    }

    public void droppedEvent(String streamName) {
        droppedStreamMeters.get(streamName).mark();
    }

    public void createRemoteStreamMeters(String remoteClusterName, int partitionCount) {
        Meter[] meters = new Meter[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            meters[i] = Metrics.newMeter(RemoteSender.class, "remote-sender@" + remoteClusterName + "@partition-" + i,
                    "sent", TimeUnit.SECONDS);
        }
        synchronized (remoteSenderMeters) {
            remoteSenderMeters.put(remoteClusterName, meters);
        }

    }

    public void sentEventToRemoteCluster(String remoteClusterName, int partition) {
        remoteSenderMeters.get(remoteClusterName)[partition].mark();
    }

    public static class CheckpointingMetrics {

        static Meter rejectedSerializationTask = Metrics.newMeter(CheckpointingMetrics.class,
                "checkpointing-rejected-serialization-task", "checkpointing-rejected-serialization-task",
                TimeUnit.SECONDS);
        static Meter rejectedStorageTask = Metrics.newMeter(CheckpointingMetrics.class,
                "checkpointing-rejected-storage-task", "checkpointing-rejected-storage-task", TimeUnit.SECONDS);
        static Meter fetchedCheckpoint = Metrics.newMeter(CheckpointingMetrics.class,
                "checkpointing-fetched-checkpoint", "checkpointing-fetched-checkpoint", TimeUnit.SECONDS);
        static Meter fetchedCheckpointFailure = Metrics.newMeter(CheckpointingMetrics.class,
                "checkpointing-fetched-checkpoint-failed", "checkpointing-fetched-checkpoint-failed", TimeUnit.SECONDS);

        public static void rejectedSerializationTask() {
            rejectedSerializationTask.mark();
        }

        public static void rejectedStorageTask() {
            rejectedStorageTask.mark();
        }

        public static void fetchedCheckpoint() {
            fetchedCheckpoint.mark();
        }

        public static void checkpointFetchFailed() {
            fetchedCheckpointFailure.mark();
        }
    }

}
