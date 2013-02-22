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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.s4.base.Destination;
import org.apache.s4.comm.helix.S4HelixConstants;
import org.apache.s4.comm.tcp.TCPDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Represents a logical cluster definition fetched from Zookeeper. Notifies
 * listeners of runtime changes in the configuration.
 * 
 */
public class ClusterFromHelix extends RoutingTableProvider implements Cluster {

    private static Logger logger = LoggerFactory
            .getLogger(ClusterFromHelix.class);

    private final String clusterName;
    private final AtomicReference<PhysicalCluster> clusterRef;
    private final List<ClusterChangeListener> listeners;
    private final Lock lock;
    private final AtomicReference<Map<String, Integer>> partitionCountMapRef;
    // Map of destination type to streamName to partitionId to Destination
    private final AtomicReference<Map<String, Map<String, Map<Integer, Destination>>>> destinationInfoMapRef;

    /**
     * only the local topology
     */
    @Inject
    public ClusterFromHelix(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout)
            throws Exception {
        this.clusterName = clusterName;
        Map<String, Integer> map = Collections.emptyMap();
        partitionCountMapRef = new AtomicReference<Map<String, Integer>>(map);
        this.clusterRef = new AtomicReference<PhysicalCluster>();
        this.listeners = new ArrayList<ClusterChangeListener>();
        Map<String, Map<String, Map<Integer, Destination>>> destinationMap = Collections
                .emptyMap();
        destinationInfoMapRef = new AtomicReference<Map<String, Map<String, Map<Integer, Destination>>>>(
                destinationMap);
        lock = new ReentrantLock();

    }

    /**
     * any topology
     */
    public ClusterFromHelix(String clusterName, ZkClient zkClient,
            String machineId) {
        this.clusterName = clusterName;
        Map<String, Integer> map = Collections.emptyMap();
        partitionCountMapRef = new AtomicReference<Map<String, Integer>>(map);
        this.clusterRef = new AtomicReference<PhysicalCluster>();
        this.listeners = new ArrayList<ClusterChangeListener>();
        Map<String, Map<String, Map<Integer, Destination>>> destinationMap = Collections
                .emptyMap();
        destinationInfoMapRef = new AtomicReference<Map<String, Map<String, Map<Integer, Destination>>>>(
                destinationMap);
        lock = new ReentrantLock();

    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList,
            NotificationContext changeContext) {
        lock.lock();
        try {
            logger.info("Start:Processing change in cluster topology");
            super.onExternalViewChange(externalViewList, changeContext);
            HelixManager manager = changeContext.getManager();
            HelixDataAccessor helixDataAccessor = manager
                    .getHelixDataAccessor();
            ConfigAccessor configAccessor = manager.getConfigAccessor();
            ConfigScopeBuilder builder = new ConfigScopeBuilder();
            Builder keyBuilder = helixDataAccessor.keyBuilder();
            List<String> resources = helixDataAccessor.getChildNames(keyBuilder
                    .idealStates());
            Map<String, Integer> partitionCountMap = new HashMap<String, Integer>();

            // populate the destinationRoutingMap
            Map<String, Map<String, Map<Integer, Destination>>> destinationRoutingMap;
            destinationRoutingMap = new HashMap<String, Map<String, Map<Integer, Destination>>>();

            List<InstanceConfig> configList = helixDataAccessor
                    .getChildValues(keyBuilder.instanceConfigs());
            Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();
            Map<String, Destination> tcpDestinationMap = new HashMap<String, Destination>();

            Map<String, Destination> udpDestinationMap = new HashMap<String, Destination>();

            for (InstanceConfig config : configList) {
                instanceConfigMap.put(config.getId(), config);
                try {
                    int port = Integer.parseInt(config.getPort());
                    Destination destination = new TCPDestination(-1, port,
                            config.getHostName(), config.getId());
                    tcpDestinationMap.put(config.getId(), destination);
                    udpDestinationMap.put(config.getId(), destination);
                } catch (NumberFormatException e) {
                    logger.error("Invalid port:" + config, e);
                }
            }
            if (externalViewList != null) {
                for (ExternalView extView : externalViewList) {
                    String resource = extView.getId();
                    ConfigScope resourceScope = builder.forCluster(S4HelixConstants.HELIX_CLUSTER_NAME)
                            .forResource(resource).build();
                    String resourceType = configAccessor.get(resourceScope,
                            "type");
                    if (!"Task".equalsIgnoreCase(resourceType)) {
                        continue;
                    }
                    String streamName = configAccessor.get(resourceScope,
                            "streamName");
                    IdealState idealstate = helixDataAccessor
                            .getProperty(keyBuilder.idealStates(resource));
                    partitionCountMap.put(streamName,
                            idealstate.getNumPartitions());
                    for (String partitionName : extView.getPartitionSet()) {
                        Map<String, String> stateMap = extView
                                .getStateMap(partitionName);
                        for (String instanceName : stateMap.keySet()) {
                            String currentState = stateMap.get(instanceName);
                            if (!currentState.equals("LEADER")) {
                                continue;
                            }
                            if (instanceConfigMap.containsKey(instanceName)) {
                                InstanceConfig instanceConfig = instanceConfigMap
                                        .get(instanceName);
                                String destinationType = "tcp";
                                addDestination(destinationRoutingMap,
                                        streamName, partitionName,
                                        "tcp", tcpDestinationMap
                                        .get(instanceConfig.getId()));
                                addDestination(destinationRoutingMap,
                                        streamName, partitionName,
                                        "tcp", udpDestinationMap
                                        .get(instanceConfig.getId()));
                            } else {
                                logger.error("Invalid instance name."
                                        + instanceName
                                        + " .Not found in /cluster/configs/. instanceName: ");
                            }
                        }
                    }
                }
            }
            destinationInfoMapRef.set(destinationRoutingMap);
            partitionCountMapRef.set(partitionCountMap);

            for (ClusterChangeListener listener : listeners) {
                listener.onChange();
            }
            logger.info("End:Processing change in cluster topology:"+partitionCountMapRef);

        } catch (Exception e) {
            logger.error("", e);
        } finally {
            lock.unlock();
        }
    }

    private void addDestination(
            Map<String, Map<String, Map<Integer, Destination>>> destinationRoutingMap,
            String streamName, String partitionName, String destinationType,
            Destination destination) {
        if (!destinationRoutingMap
                .containsKey(destinationType)) {
            destinationRoutingMap
                    .put(destinationType,
                            new HashMap<String, Map<Integer, Destination>>());
        }
        Map<String, Map<Integer, Destination>> typeMap = destinationRoutingMap
                .get(destinationType);
        if (!typeMap.containsKey(streamName)) {
            typeMap.put(streamName,
                    new HashMap<Integer, Destination>());
        }
        Map<Integer, Destination> streamMap = typeMap
                .get(streamName);
        String[] split = partitionName.split("_");
        if (split.length == 2) {
            try {
                int partitionId = Integer
                        .parseInt(split[1]);
                streamMap.put(partitionId, destination);
            } catch (NumberFormatException e) {

            }
        }
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((clusterName == null) ? 0 : clusterName.hashCode());
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
        ClusterFromHelix other = (ClusterFromHelix) obj;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        return true;
    }

    @Override
    public Destination getDestination(String streamName, int partitionId,
            String destinationType) {

        Map<String, Map<Integer, Destination>> typeMap = destinationInfoMapRef
                .get().get(destinationType);
        if (typeMap == null)
            return null;
        
        Map<Integer, Destination> streamMap = typeMap.get(streamName);
        if(streamMap==null){
            streamMap = typeMap.get(S4HelixConstants.GLOBAL_TASK_NAME);
        }
        if (streamMap == null)
            return null;

        return streamMap.get(partitionId);
    }

    @Override
    public Integer getPartitionCount(String streamName) {
        
        Integer numPartitions;
        numPartitions = partitionCountMapRef.get().get(streamName);
        if(numPartitions==null){
            numPartitions = partitionCountMapRef.get().get(S4HelixConstants.GLOBAL_TASK_NAME);
        }
        if (numPartitions == null) {
            return -1;
        }
        return numPartitions;
    }

}
