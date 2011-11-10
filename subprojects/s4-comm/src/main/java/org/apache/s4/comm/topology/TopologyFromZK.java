package org.apache.s4.comm.topology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class TopologyFromZK implements Topology, IZkChildListener, IZkStateListener, IZkDataListener {
    private static final Logger logger = LoggerFactory.getLogger(TopologyFromZK.class);
    private final String clusterName;
    private final AtomicReference<Cluster> clusterRef;
    private final List<TopologyChangeListener> listeners;
    private KeeperState state;
    private final ZkClient zkClient;
    private final String taskPath;
    private final String processPath;
    private final Lock lock;
    private AtomicBoolean currentlyOwningTask;
    private String machineId;

    @Inject
    public TopologyFromZK(@Named("cluster.name") String clusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) throws Exception {
        this.clusterName = clusterName;
        taskPath = "/" + clusterName + "/" + "tasks";
        processPath = "/" + clusterName + "/" + "process";
        lock = new ReentrantLock();
        clusterRef = new AtomicReference<Cluster>();
        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
        listeners = new ArrayList<TopologyChangeListener>();
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

    @Override
    public Cluster getTopology() {
        return clusterRef.get();
    }

    @Override
    public void addListener(TopologyChangeListener listener) {
        logger.info("Adding topology change listener:" + listener);
        listeners.add(listener);
    }

    @Override
    public void removeListener(TopologyChangeListener listener) {
        logger.info("Removing topology change listener:" + listener);
        listeners.remove(listener);
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        doProcess();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
        doProcess();
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        this.state = state;
    }

    @Override
    public void handleNewSession() throws Exception {
        logger.info("New session:" + zkClient.getSessionId());
        zkClient.subscribeChildChanges(taskPath, this);
        zkClient.subscribeChildChanges(processPath, this);
        doProcess();
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        doProcess();
    }

    private void doProcess() {
        lock.lock();
        try {
            refreshTopology();
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            lock.unlock();
        }
    }

    private void refreshTopology() throws Exception {
        List<String> processes = zkClient.getChildren(processPath);
        List<String> tasks = zkClient.getChildren(taskPath);
        Cluster cluster = new Cluster(tasks.size());
        for (int i = 0; i < processes.size(); i++) {
            ZNRecord process = zkClient.readData(processPath + "/" + processes.get(i), true);
            if (process != null) {
                int partition = Integer.parseInt(process.getSimpleField("partition"));
                String host = process.getSimpleField("host");
                int port = Integer.parseInt(process.getSimpleField("port"));
                String taskId = process.getSimpleField("taskId");
                ClusterNode node = new ClusterNode(partition, port, host, taskId);
                cluster.addNode(node);
            }
        }
        logger.info("Changing cluster topology to " + cluster + " from " + clusterRef.get());
        clusterRef.set(cluster);
        // Notify all changeListeners about the topology change
        for (TopologyChangeListener listener : listeners) {
            listener.onChange();
        }
    }
}
