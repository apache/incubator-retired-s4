package io.s4.comm.topology;

import io.s4.comm.topology.Cluster;

public interface Topology {
    public Cluster getTopology();
    public void addListener(TopologyChangeListener listener);
    public void removeListener(TopologyChangeListener listener);
}
