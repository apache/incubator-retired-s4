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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Monitors all clusters
 * 
 */
@Singleton
public class ClustersFromZK implements Clusters, IZkStateListener {
    private static final Logger logger = LoggerFactory.getLogger(ClustersFromZK.class);
    private KeeperState state;
    private final ZkClient zkClient;
    private final Lock lock;
    private String machineId;
    private final Map<String, ClusterFromZK> clusters = new HashMap<String, ClusterFromZK>();
    private final int connectionTimeout;
    private final String clusterName;

    @Inject
    public ClustersFromZK(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, ZkClient zkClient) throws Exception {
        this.clusterName = clusterName;
        this.connectionTimeout = connectionTimeout;
        lock = new ReentrantLock();
        this.zkClient = zkClient;
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
        if (state.equals(KeeperState.Expired)) {
            logger.error(
                    "Zookeeper session expired, possibly due to a network partition for cluster [{}]. This node is considered as dead by Zookeeper. Proceeding to stop this node.",
                    clusterName);
            System.exit(1);
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

    @Override
    public Cluster getCluster(String clusterName) {
        return clusters.get(clusterName);
    }

}
