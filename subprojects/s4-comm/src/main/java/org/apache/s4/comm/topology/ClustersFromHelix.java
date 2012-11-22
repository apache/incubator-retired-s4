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
public class ClustersFromHelix implements Clusters {
    private static final Logger logger = LoggerFactory.getLogger(ClustersFromHelix.class);
    private String machineId;
    private Map<String, ClusterFromHelix> clusters = new HashMap<String, ClusterFromHelix>();
    private int connectionTimeout;
    private String clusterName;

    @Inject
    public ClustersFromHelix(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout) throws Exception {
        this.clusterName = clusterName;
        this.connectionTimeout = connectionTimeout;


    }

  

    public Cluster getCluster(String clusterName) {
        return clusters.get(clusterName);
    }

}
