/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Handles partition assignment through Zookeeper.
 * 
 */
@Singleton
public class AssignmentFromZK implements Assignment, IZkChildListener, IZkStateListener, IZkDataListener {
    private static final Logger logger = LoggerFactory.getLogger(AssignmentFromZK.class);
    /**
     * Current state of connection with ZK
     */
    private KeeperState state;
    /**
     * ZkClient used to do all interactions with Zookeeper
     */
    private final ZkClient zkClient;
    /**
     * Root path of tasks in ZK
     */
    private final String taskPath;
    /**
     * Root path of processes in ZK
     */
    private final String processPath;
    /**
     * Reentrant lock used to synchronize processing callback
     */
    private final Lock lock;
    /**
     * Variable that indicates if this instance is currently owning any task.
     */
    private final AtomicBoolean currentlyOwningTask;
    /**
     * Hostname where the process is running
     */
    private String machineId;
    /**
     * Condition to signal taskAcquisition
     */
    private final Condition taskAcquired;
    /**
     * Holds the reference to ClusterNode which points to the current partition owned
     */
    AtomicReference<ClusterNode> clusterNodeRef;
    private final int connectionTimeout;
    private final String clusterName;

    // TODO we currently have a single assignment per node (i.e. a node can only belong to 1 topology)
    @Inject
    public AssignmentFromZK(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, ZkClient zkClient) throws Exception {
        this.clusterName = clusterName;
        this.connectionTimeout = connectionTimeout;
        taskPath = "/s4/clusters/" + clusterName + "/tasks";
        processPath = "/s4/clusters/" + clusterName + "/process";
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

        this.zkClient = zkClient;
    }

    @Inject
    void init() throws Exception {
        zkClient.subscribeStateChanges(this);
        if (!zkClient.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS)) {
            throw new Exception("cannot connect to zookeeper");
        }
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
        if (state.equals(KeeperState.Expired)) {
            logger.error(
                    "Zookeeper session expired, possibly due to a network partition for cluster [{}]. This node is considered as dead by Zookeeper. Proceeding to stop this node.",
                    clusterName);
            System.exit(1);
        }
    }

    @Override
    public void handleNewSession() {
        logger.info("New session:" + zkClient.getSessionId() + "; state is : " + state.name());
        currentlyOwningTask.set(false);
        zkClient.subscribeChildChanges(taskPath, this);
        zkClient.subscribeChildChanges(processPath, this);
        doProcess();
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        doProcess();
    }

    /**
     * One method to do any processing if there is a change in ZK, all callbacks will be processed sequentially
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
     * Core method where the task acquisition happens. Algo is as follow Get All the tasks<br/>
     * Get All the processes<br/>
     * Check if the number of process is less than task<br/>
     * Iterate over the tasks and pick up one that is not yet acquire<br/>
     * 
     * If the creation of ephemeral process node is successful then task acquisition is successful
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
                    ZNRecord task = zkClient.readData(taskPath + "/" + taskName);
                    ZNRecord process = new ZNRecord(task);
                    process.putSimpleField("host", machineId);
                    process.putSimpleField("session", String.valueOf(zkClient.getSessionId()));
                    try {
                        zkClient.createEphemeral(processPath + "/" + taskName, process);

                    } catch (Throwable e) {
                        if (e instanceof ZkNodeExistsException) {
                            logger.trace("Task already created");
                        } else {
                            logger.debug("Exception trying to acquire task:" + taskName
                                    + " This is warning and can be ignored. " + e);
                            // Any exception does not means we failed to acquire
                            // task because we might have acquired task but there
                            // was ZK connection loss
                            // We will check again in the next section if we created
                            // the process node successfully
                        }
                    }
                    // check if the process node is created and we own it
                    Stat stat = zkClient.getStat(processPath + "/" + taskName);
                    if (stat != null && stat.getEphemeralOwner() == zkClient.getSessionId()) {
                        logger.info("Successfully acquired task:" + taskName + " by " + machineId);
                        int partition = Integer.parseInt(process.getSimpleField("partition"));
                        String host = process.getSimpleField("host");
                        int port = Integer.parseInt(process.getSimpleField("port"));
                        String taskId = process.getSimpleField("taskId");
                        ClusterNode node = new ClusterNode(partition, port, host, taskId);
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
