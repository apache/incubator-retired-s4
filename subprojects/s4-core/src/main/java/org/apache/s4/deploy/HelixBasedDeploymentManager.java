package org.apache.s4.deploy;

import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.Server;
import org.apache.s4.core.util.AppConfig;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HelixBasedDeploymentManager implements DeploymentManager {
    private final Server server;
    boolean deployed = false;
    private final String clusterName;
	private ArchiveFetcher fetcher;

    @Inject
    public HelixBasedDeploymentManager(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, Server server, ArchiveFetcher fetcher) {
        this.clusterName = clusterName;
        this.server = server;
		this.fetcher = fetcher;

    }

    @Override
    public void start() {
        
    }

	@Override
	public void deploy(AppConfig appConfig) throws DeploymentFailedException {
		DeploymentUtils.deploy(server, fetcher, clusterName, appConfig);
	}

	@Override
	public void undeploy(AppConfig appConfig) {
		
	}

}
