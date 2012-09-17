package org.apache.s4.core.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Emitter;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Receiver;
import org.apache.s4.core.RemoteSender;
import org.apache.s4.core.Sender;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

@Singleton
public class S4Metrics {

    private static Logger logger = LoggerFactory.getLogger(S4Metrics.class);

    @Inject
    Emitter emitter;

    @Inject
    Assignment assignment;

    static List<Meter> partitionSenderMeters = Lists.newArrayList();

    private static Meter eventMeter = Metrics.newMeter(Receiver.class, "received-events", "event-count",
            TimeUnit.SECONDS);
    private static Meter bytesMeter = Metrics.newMeter(Receiver.class, "received-bytes", "bytes-count",
            TimeUnit.SECONDS);

    private static Meter[] senderMeters;

    private static Map<String, Meter> dequeuingStreamMeters = Maps.newHashMap();
    private static Map<String, Meter> droppedStreamMeters = Maps.newHashMap();

    private static Map<String, Meter[]> remoteSenderMeters = Maps.newHashMap();

    @Inject
    private void init() {
        senderMeters = new Meter[emitter.getPartitionCount()];
        int localPartitionId = assignment.assignClusterNode().getPartition();
        for (int i = 0; i < senderMeters.length; i++) {
            senderMeters[i] = Metrics.newMeter(Sender.class, "sender", "sent-to-"
                    + ((i == localPartitionId) ? i + "(local)" : i), TimeUnit.SECONDS);
        }
    }

    public static void createCacheGauges(ProcessingElement prototype,
            final LoadingCache<String, ProcessingElement> cache) {

        Metrics.newGauge(prototype.getClass(), "PE-cache-entries", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.size();
            }
        });
        Metrics.newGauge(prototype.getClass(), "PE-cache-evictions", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.stats().evictionCount();
            }
        });
        Metrics.newGauge(prototype.getClass(), "PE-cache-misses", new Gauge<Long>() {

            @Override
            public Long value() {
                return cache.stats().missCount();
            }
        });
    }

    public static void receivedEvent(int bytes) {
        eventMeter.mark();
        bytesMeter.mark(bytes);
    }

    public static void sentEvent(int partition) {
        try {
            senderMeters[partition].mark();
        } catch (NullPointerException e) {
            logger.warn("Sender meter not ready for partition {}", partition);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Partition {} does not exist", partition);
        }
    }

    public static void createStreamMeters(String name) {
        // TODO avoid maps to avoid map lookups?
        dequeuingStreamMeters.put(name,
                Metrics.newMeter(Stream.class, "dequeued@" + name, "dequeued", TimeUnit.SECONDS));
        droppedStreamMeters.put(name, Metrics.newMeter(Stream.class, "dropped@" + name, "dropped", TimeUnit.SECONDS));

    }

    public static void dequeuedEvent(String name) {
        dequeuingStreamMeters.get(name).mark();
    }

    public static void createRemoteStreamMeters(String remoteClusterName, int partitionCount) {
        Meter[] meters = new Meter[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            meters[i] = Metrics.newMeter(RemoteSender.class, "remote-sender@" + remoteClusterName + "@partition-" + i,
                    "sent", TimeUnit.SECONDS);
        }
        synchronized (remoteSenderMeters) {
            remoteSenderMeters.put(remoteClusterName, meters);
        }

    }

    public static void sentEventToRemoteCluster(String remoteClusterName, int partition) {
        remoteSenderMeters.get(remoteClusterName)[partition].mark();
    }

    public static class CheckpointingMetrics {

        static Meter rejectedSerializationTask = Metrics.newMeter(CheckpointingMetrics.class, "checkpointing",
                "rejected-serialization-task", TimeUnit.SECONDS);
        static Meter rejectedStorageTask = Metrics.newMeter(CheckpointingMetrics.class, "checkpointing",
                "rejected-storage-task", TimeUnit.SECONDS);
        static Meter fetchedCheckpoint = Metrics.newMeter(CheckpointingMetrics.class, "checkpointing",
                "fetched-checkpoint", TimeUnit.SECONDS);
        static Meter fetchedCheckpointFailure = Metrics.newMeter(CheckpointingMetrics.class, "checkpointing",
                "fetched-checkpoint-failed", TimeUnit.SECONDS);

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
