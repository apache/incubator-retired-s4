package org.apache.s4.comm.topology;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class RemoteTopologyFromZK extends TopologyFromZK implements RemoteTopology {

    @Inject
    public RemoteTopologyFromZK(@Named("cluster.remote.name") String remoteClusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) throws Exception {
        super(remoteClusterName, zookeeperAddress, sessionTimeout, connectionTimeout);
    }

}
