package org.apache.s4.core;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * 
 * Provides a connection to ZooKeeper through the {@link ZkClient} class.
 * 
 * As an application singleton, it provides a single shared connection for an S4 node.
 * 
 */
@Singleton
// injected only once per node
public class ZkClientProvider implements Provider<ZkClient> {

    private final ZkClient zkClient;

    @Inject
    public ZkClientProvider(@Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout) {
        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
    }

    @Override
    public ZkClient get() {
        // reuses initialized instance
        return zkClient;

    }
}
