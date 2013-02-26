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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Represents a logical cluster definition fetched from Zookeeper. Notifies listeners of runtime changes in the
 * configuration.
 * 
 */
@Singleton
public class ClusterFromZK implements Cluster, IZkChildListener, IZkDataListener, IZkStateListener {

    private static Logger logger = LoggerFactory.getLogger(ClusterFromZK.class);

    private final AtomicReference<PhysicalCluster> clusterRef;
    private final List<ClusterChangeListener> listeners;
    private final ZkClient zkClient;
    private final String taskPath;
    private final String processPath;
    private final Lock lock;
    private final String clusterName;

    /**
     * only the local topology
     */
    @Inject
    public ClusterFromZK(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, ZkClient zkClient) throws Exception {
        this.clusterName = clusterName;
        this.taskPath = "/s4/clusters/" + clusterName + "/tasks";
        this.processPath = "/s4/clusters/" + clusterName + "/process";
        lock = new ReentrantLock();
        this.zkClient = zkClient;
        zkClient.subscribeStateChanges(this);
        if (!zkClient.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS)) {
            throw new Exception("cannot connect to zookeeper");
        }
        try {
            InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn("Unable to get hostname", e);
        }
        this.clusterRef = new AtomicReference<PhysicalCluster>();
        this.listeners = new ArrayList<ClusterChangeListener>();
        this.handleStateChanged(KeeperState.SyncConnected);
        zkClient.subscribeChildChanges(taskPath, this);
        zkClient.subscribeChildChanges(processPath, this);
        // bug in zkClient, it does not invoke handleNewSession the first time
        // it connects
        this.handleNewSession();

    }

    /**
     * any topology
     */
    public ClusterFromZK(String clusterName, ZkClient zkClient, String machineId) {

        this.zkClient = zkClient;
        this.taskPath = "/s4/clusters/" + clusterName + "/tasks";
        this.processPath = "/s4/clusters/" + clusterName + "/process";
        this.clusterName = clusterName;
        this.lock = new ReentrantLock();
        this.listeners = new ArrayList<ClusterChangeListener>();
        this.clusterRef = new AtomicReference<PhysicalCluster>();
        zkClient.subscribeChildChanges(taskPath, this);
        zkClient.subscribeChildChanges(processPath, this);
    }

    @Override
    public PhysicalCluster getPhysicalCluster() {
        return clusterRef.get();
    }

    @Override
    public void addListener(ClusterChangeListener listener) {
        logger.info("Adding topology change listener:" + listener);
        listeners.add(listener);
    }

    @Override
    public void removeListener(ClusterChangeListener listener) {
        logger.info("Removing topology change listener:" + listener);
        listeners.remove(listener);
    }

    @Override
    public void handleChildChange(String paramString, List<String> paramList) throws Exception {
        doProcess();
    }

    void doProcess() {
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
        PhysicalCluster cluster = new PhysicalCluster(tasks.size());
        for (int i = 0; i < processes.size(); i++) {
            cluster.setName(clusterName);
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
        for (ClusterChangeListener listener : listeners) {
            listener.onChange();
        }
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClusterFromZK other = (ClusterFromZK) obj;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        return true;
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        if (state.equals(KeeperState.Expired)) {
            logger.error(
                    "Zookeeper session expired, possibly due to a network partition for cluster [{}]. This node is considered as dead by Zookeeper. Proceeding to stop this node.",
                    clusterRef.get().toString());
            System.exit(1);
        }
    }

    @Override
    public void handleNewSession() throws Exception {
        doProcess();

    }

}
