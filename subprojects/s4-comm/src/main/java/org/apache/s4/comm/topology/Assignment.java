package org.apache.s4.comm.topology;

/**
 * 
 * Upon startup an S4 process in a cluster must be assigned one and only one of
 * the available cluster nodes. Cluster nodes ({@link ClusterNode}) are defined
 * using a configuration mechanism at startup.
 * 
 * The Assignment implementation is responsible for coordinating how cluster
 * nodes are uniquely assigned to processes.
 * 
 */
public interface Assignment {

    /**
     * @return the ClusterNode associated assigned to this process.
     */
    public ClusterNode assignClusterNode();
}
