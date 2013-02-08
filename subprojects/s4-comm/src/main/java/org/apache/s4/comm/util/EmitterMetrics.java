package org.apache.s4.comm.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Destination;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

public class EmitterMetrics {
    private final Map<String, Map<String, Meter>> emittersMetersMap;
    private Cluster cluster;

    public EmitterMetrics(Cluster cluster) {
        this.cluster = cluster;
        emittersMetersMap = new HashMap<String, Map<String, Meter>>();
    }

    public void sentMessage(Destination destination) {
        //TODO:
        /*
        Map<String, Meter> map = emittersMetersMap.get(stream);
        if (map == null) {
            map = new HashMap<String, Meter>();
            emittersMetersMap.put(stream, map);
        }
        Meter meter = emittersMetersMap.get(stream).get(partitionId);
        if (meter == null) {
            meter = Metrics.newMeter(TCPEmitter.class, "event-emitted@"
                    + cluster.getPhysicalCluster().getName() + "@stream-"+ stream + "@partition-"
                    + partitionId, "event-emitted", TimeUnit.SECONDS);
            emittersMetersMap.get(stream).put(partitionId, meter);
        }
        */
    }
}
