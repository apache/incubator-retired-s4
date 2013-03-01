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

    /**
     * Lists consumers of a given stream
     */
    public abstract Set<StreamConsumer> getConsumers(String streamName);

    /**
     * Publishes availability of an output stream
     * 
     * @param clusterName
     *            originating cluster
     * @param streamName
     *            name of stream
     */
    public abstract void addOutputStream(String clusterName, String streamName);

    /**
     * Publishes interest in a stream, by a given cluster
     * 
     * @param clusterName
     * @param streamName
     */
    public abstract void addInputStream(String clusterName, String streamName);

}
