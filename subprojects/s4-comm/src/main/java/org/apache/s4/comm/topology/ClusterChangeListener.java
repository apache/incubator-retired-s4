package org.apache.s4.comm.topology;

/**
 * Entities interested in changes occurring in topologies implement this listener and should register through the
 * {@link Cluster} interface
 * 
 */
public interface ClusterChangeListener {
    public void onChange();
}
