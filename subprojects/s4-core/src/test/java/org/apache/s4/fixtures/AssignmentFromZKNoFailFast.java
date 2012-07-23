package org.apache.s4.fixtures;

import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class AssignmentFromZKNoFailFast extends AssignmentFromZK {

    @Inject
    public AssignmentFromZKNoFailFast(@Named("cluster.name") String clusterName,
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
