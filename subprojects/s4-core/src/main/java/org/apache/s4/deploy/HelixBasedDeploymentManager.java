package org.apache.s4.deploy;

import org.apache.s4.core.Server;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HelixBasedDeploymentManager implements DeploymentManager {
    private final Server server;
    boolean deployed = false;
    private final String clusterName;

    @Inject
    public HelixBasedDeploymentManager(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, Server server) {
        this.clusterName = clusterName;
        this.server = server;

    }

    @Override
    public void start() {
        
    }

}
