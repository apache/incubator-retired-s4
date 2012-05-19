package org.apache.s4.comm.topology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Monitors all clusters
 * 
 */
public class ClustersFromZK implements Clusters, IZkStateListener {
    private static final Logger logger = LoggerFactory.getLogger(ClustersFromZK.class);
    private KeeperState state;
    private final ZkClient zkClient;
    private final Lock lock;
    private String machineId;
    private Map<String, ClusterFromZK> clusters = new HashMap<String, ClusterFromZK>();
    private int connectionTimeout;
    private String clusterName;

    @Inject
    public ClustersFromZK(@Named("cluster.name") String clusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) throws Exception {
        this.clusterName = clusterName;
        this.connectionTimeout = connectionTimeout;
        lock = new ReentrantLock();
        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
        zkClient.subscribeStateChanges(this);
        zkClient.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS);
        try {
            machineId = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn("Unable to get hostname", e);
            machineId = "UNKNOWN";
        }
        // bug in zkClient, it does not invoke handleNewSession the first time
        // it connects
        this.handleStateChanged(KeeperState.SyncConnected);

        this.handleNewSession();

    }

    /**
     * One method to do any processing if there is a change in ZK, all callbacks will be processed sequentially
     */
    private void doProcess() {
        lock.lock();
        try {
            for (Map.Entry<String, ClusterFromZK> cluster : clusters.entrySet()) {
                cluster.getValue().doProcess();
            }
        } catch (Exception e) {
            logger.warn("Exception in tryToAcquireTask", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        this.state = state;
        if (!state.equals(KeeperState.SyncConnected)) {
            logger.warn("Session not connected for cluster [{}]: [{}]. Trying to reconnect", clusterName, state.name());
            zkClient.connect(connectionTimeout, null);
            handleNewSession();
        }
    }

    @Override
    public void handleNewSession() throws Exception {
        logger.info("New session:" + zkClient.getSessionId());
        List<String> clusterNames = zkClient.getChildren("/s4/clusters");
        for (String clusterName : clusterNames) {
            ClusterFromZK cluster = new ClusterFromZK(clusterName, zkClient, machineId);
            clusters.put(clusterName, cluster);
        }
        doProcess();
    }

    public Cluster getCluster(String clusterName) {
        return clusters.get(clusterName);
    }

}
