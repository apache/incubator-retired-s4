package org.apache.s4.comm.topology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
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

public class AssignmentFromZK implements Assignment, IZkChildListener,
        IZkStateListener, IZkDataListener {
    private static final Logger logger = LoggerFactory
            .getLogger(TopologyFromZK.class);
    /*
     * Name of the cluster
     */
    private final String clusterName;
    /**
     * Current state of connection with ZK
     */
    private KeeperState state;
    /**
     * ZkClient used to do all interactions with Zookeeper
     */
    private ZkClient zkClient;
    /**
     * Root path of tasks in ZK
     */
    private String taskPath;
    /**
     * Root path of processes in ZK
     */
    private String processPath;
    /**
     * Reentrant lock used to synchronize processing callback
     */
    private Lock lock;
    /**
     * Variable that indicates if this instance is currently owning any task.
     */
    private AtomicBoolean currentlyOwningTask;
    /**
     * Hostname where the process is running
     */
    private String machineId;
    /**
     * Condition to signal taskAcquisition
     */
    private Condition taskAcquired;
    /**
     * Holds the reference to ClusterNode which points to the current partition
     * owned
     */
    AtomicReference<ClusterNode> clusterNodeRef;

    public AssignmentFromZK(String clusterName, String zookeeperAddress,
            int sessionTimeout, int connectionTimeout) throws Exception {
        this.clusterName = clusterName;
        taskPath = "/" + clusterName + "/" + "tasks";
        processPath = "/" + clusterName + "/" + "process";
        lock = new ReentrantLock();
        clusterNodeRef = new AtomicReference<ClusterNode>();
        taskAcquired = lock.newCondition();
        currentlyOwningTask = new AtomicBoolean(false);

        try {
            machineId = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn("Unable to get hostname", e);
            machineId = "UNKNOWN";
        }

        zkClient = new ZkClient(zookeeperAddress, sessionTimeout,
                connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
        zkClient.subscribeStateChanges(this);
        zkClient.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS);
        // bug in zkClient, it does not invoke handleNewSession the first time
        // it connects
        this.handleStateChanged(KeeperState.SyncConnected);
        this.handleNewSession();
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
    public void handleNewSession() {
        logger.info("New session:" + zkClient.getSessionId());
        currentlyOwningTask.set(false);
        zkClient.subscribeChildChanges(taskPath, this);
        zkClient.subscribeChildChanges(processPath, this);
        doProcess();
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception {
        doProcess();
    }

    /**
     * One method to do any processing if there is a change in ZK, all callbacks
     * will be processed sequentially
     */
    private void doProcess() {
        lock.lock();
        try {
            // tryToAcquire new task only if currently not holding anything
            if (!currentlyOwningTask.get()) {
                tryToAcquiretask();
                if (!currentlyOwningTask.get()) {
                    logger.info("Could not acquire task. Going into standby mode");
                }

            }
        } catch (Exception e) {
            logger.warn("Exception in tryToAcquireTask", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Core method where the task acquisition happens. Algo is as follow Get All
     * the tasks<br/>
     * Get All the processes<br/>
     * Check if the number of process is less than task<br/>
     * Iterate over the tasks and pick up one that is not yet acquire<br/>
     * 
     * If the creation of ephemeral process node is successful then task
     * acquisition is successful
     */
    private void tryToAcquiretask() {
        List<String> tasks = zkClient.getChildren(taskPath);
        List<String> processes = zkClient.getChildren(processPath);
        // check if the number of process is less than tasks
        if (processes.size() < tasks.size()) {
            // if yes, go over the tasks
            for (int i = 0; i < tasks.size(); i++) {
                String taskName = tasks.get(i);
                if (processes.contains(taskName)) {
                    continue;
                }
                if (!zkClient.exists(processPath + "/" + taskName)) {
                    ZNRecord task = zkClient
                            .readData(taskPath + "/" + taskName);
                    ZNRecord process = new ZNRecord(task);
                    process.setSimpleField("host", machineId);
                    process.setSimpleField("session",
                            String.valueOf(zkClient.getSessionId()));
                    try {
                        zkClient.createEphemeral(processPath + "/" + taskName,
                                process);

                    } catch (Throwable e) {
                        logger.warn("Exception trying to acquire task. This is warning and can be ignored"
                                + e);
                        // Any exception does not means we failed to acquire
                        // task because we might have acquired task but there
                        // was ZK connection loss
                        // We will check again in the next section if we created
                        // the process node successfully
                    }
                    // check if the process node is created and we own it
                    Stat stat = zkClient.getStat(processPath + "/" + taskName);
                    if (stat != null
                            && stat.getEphemeralOwner() == zkClient
                                    .getSessionId()) {
                        logger.info("Successfully acquired task:" + taskName
                                + " by " + machineId);
                        int partition = Integer.parseInt(process
                                .getSimpleField("partition"));
                        String host = process.getSimpleField("host");
                        int port = Integer.parseInt(process
                                .getSimpleField("port"));
                        String taskId = process.getSimpleField("taskId");
                        ClusterNode node = new ClusterNode(partition, port,
                                host, taskId);
                        clusterNodeRef.set(node);
                        currentlyOwningTask.set(true);
                        taskAcquired.signalAll();
                        break;
                    }
                }

            }
        }
    }

    @Override
    public ClusterNode assignClusterNode() {
        lock.lock();
        try {
            while (!currentlyOwningTask.get()) {
                taskAcquired.awaitUninterruptibly();
            }
        } catch (Exception e) {
            logger.error("Exception while waiting for task to be acquired");
            return null;
        } finally {
            lock.unlock();
        }
        return clusterNodeRef.get();
    }

}
