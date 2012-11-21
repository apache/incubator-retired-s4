package org.apache.s4.comm.util;

import java.util.concurrent.TimeUnit;

import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

public class EmitterMetrics {
    private final Meter[] emittersMeters;

    public EmitterMetrics(Cluster cluster) {
        emittersMeters = new Meter[cluster.getPhysicalCluster().getPartitionCount()];
        for (int i = 0; i < cluster.getPhysicalCluster().getPartitionCount(); i++) {
            emittersMeters[i] = Metrics.newMeter(TCPEmitter.class, "event-emitted@"
                    + cluster.getPhysicalCluster().getName() + "@partition-" + i, "event-emitted", TimeUnit.SECONDS);
        }
    }

    public void sentMessage(int partitionId) {
        emittersMeters[partitionId].mark();
    }
}
