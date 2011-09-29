package io.s4.comm.topology;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class TopologyFromFile implements Topology {
    private String clusterName;
    private String clusterConfigurationFilename;
    private Cluster cluster;

    @Inject
    public TopologyFromFile(@Named("comm.cluster_name") String clusterName,
            @Named("comm.cluster_config") String clusterConfigurationFilename) {
        super();
        this.clusterName = clusterName;
        this.clusterConfigurationFilename = clusterConfigurationFilename;
        readStaticConfig();

    }

    private void readStaticConfig() {
        ConfigParser parser = new ConfigParser();
        Config config = parser.parse(clusterConfigurationFilename);

        // find the requested cluster
        for (Cluster checkCluster : config.getClusters()) {
            if (checkCluster.getName().equals(clusterName)) {
                cluster = checkCluster;
                break;
            }
        }
        if (cluster == null) {
            throw new RuntimeException("Cluster " + clusterName
                    + " not configured");
        }
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
