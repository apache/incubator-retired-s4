package org.apache.s4.comm.topology;

import java.util.Set;

public interface RemoteStreams {

    public abstract Set<StreamConsumer> getConsumers(String streamName);

    public abstract void addOutputStream(String appId, String clusterName, String streamName);

    /**
     * Publishes interest in a stream from an application.
     * 
     * @param appId
     * @param clusterName
     * @param streamName
     */
    public abstract void addInputStream(int appId, String clusterName, String streamName);

}
