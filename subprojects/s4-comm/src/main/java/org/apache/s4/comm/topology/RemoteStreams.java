package org.apache.s4.comm.topology;

import java.util.Set;

/**
 * <p>
 * Monitors streams available in the S4 cluster.
 * </p>
 * <p>
 * Maintains a data structure reflecting the currently published streams with their consumers and publishers.
 * </p>
 * <p>
 * Provides methods to publish producers and consumers of streams
 * </p>
 * 
 */

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
