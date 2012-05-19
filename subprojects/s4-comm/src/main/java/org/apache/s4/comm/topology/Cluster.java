package org.apache.s4.comm.topology;

/**
 * Represents a logical cluster
 * 
 */
public interface Cluster {
    public PhysicalCluster getPhysicalCluster();

    public void addListener(ClusterChangeListener listener);

    public void removeListener(ClusterChangeListener listener);
}
