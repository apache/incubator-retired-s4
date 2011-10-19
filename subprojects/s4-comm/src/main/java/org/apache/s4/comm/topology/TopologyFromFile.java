package org.apache.s4.comm.topology;

import com.google.inject.Inject;

public class TopologyFromFile implements Topology {

    private Cluster cluster;

    @Inject
    public TopologyFromFile(Cluster cluster) {
        super();
        this.cluster = cluster;

    }

    @Override
    public Cluster getTopology() {
        return cluster;
    }

    @Override
    public void addListener(TopologyChangeListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeListener(TopologyChangeListener listener) {
        // TODO Auto-generated method stub

    }

}
