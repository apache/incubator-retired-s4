package org.apache.s4.fixtures;

import org.apache.s4.comm.topology.ClustersFromZK;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ClustersFromZKNoFailFast extends ClustersFromZK {

    @Inject
    public ClustersFromZKNoFailFast(@Named("cluster.name") String clusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) throws Exception {
        super(clusterName, zookeeperAddress, sessionTimeout, connectionTimeout);
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        // no fail fast
    }

}
