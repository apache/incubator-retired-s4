package org.apache.s4.comm.topology;

/**
 * A subscriber to a published stream. Identified through its cluster name (for dispatching to the remote cluster) and
 * application ID (for dispatching within a node).
 * 
 */
public class StreamConsumer {

    int appId;
    String clusterName;

    public StreamConsumer(int appId, String clusterName) {
        super();
        this.appId = appId;
        this.clusterName = clusterName;
    }

    public int getAppId() {
        return appId;
    }

    public String getClusterName() {
        return clusterName;
    }

}
