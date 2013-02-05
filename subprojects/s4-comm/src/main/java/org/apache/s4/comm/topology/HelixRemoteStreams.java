package org.apache.s4.comm.topology;

import java.util.Collections;
import java.util.Set;

public class HelixRemoteStreams implements RemoteStreams {

    @Override
    public Set<StreamConsumer> getConsumers(String streamName) {
        // TODO implement?
        return Collections.emptySet();
    }

    @Override
    public void addOutputStream(String appId, String clusterName, String streamName) {
        // TODO implement?
    }

    @Override
    public void addInputStream(int appId, String clusterName, String streamName) {
        // TODO implement?
    }

}
