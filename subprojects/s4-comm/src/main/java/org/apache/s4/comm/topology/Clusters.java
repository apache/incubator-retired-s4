package org.apache.s4.comm.topology;

/**
 * Represents clusters related to the current node (clusters to which this node belongs, and connected clusters that may
 * receive messages from this node)
 * 
 */
public interface Clusters {

    Cluster getCluster(String clusterName);

}
