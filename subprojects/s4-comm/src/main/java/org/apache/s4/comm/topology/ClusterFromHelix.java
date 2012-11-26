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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;

/**
 * Represents a logical cluster definition fetched from Zookeeper. Notifies
 * listeners of runtime changes in the configuration.
 * 
 */
public class ClusterFromHelix extends RoutingTableProvider implements Cluster {

	private static Logger logger = LoggerFactory
			.getLogger(ClusterFromHelix.class);

	private String clusterName;
	private final AtomicReference<PhysicalCluster> clusterRef;
	private final List<ClusterChangeListener> listeners;
	private final Lock lock;

	/**
	 * only the local topology
	 */
	@Inject
	public ClusterFromHelix(@Named("s4.cluster.name") String clusterName,
			@Named("s4.cluster.zk_address") String zookeeperAddress,
			@Named("s4.cluster.zk_session_timeout") int sessionTimeout,
			@Named("s4.cluster.zk_connection_timeout") int connectionTimeout)
			throws Exception {
		this.clusterRef = new AtomicReference<PhysicalCluster>();
		this.listeners = new ArrayList<ClusterChangeListener>();
		lock = new ReentrantLock();

	}

	/**
	 * any topology
	 */
	public ClusterFromHelix(String clusterName, ZkClient zkClient,
			String machineId) {
		this.clusterName = clusterName;
		this.clusterRef = new AtomicReference<PhysicalCluster>();
		this.listeners = new ArrayList<ClusterChangeListener>();
		lock = new ReentrantLock();

	}

	@Override
	public void onExternalViewChange(List<ExternalView> externalViewList,
			NotificationContext changeContext) {
		lock.lock();
		try {
		  logger.info("Start:Processing change in cluster topology");
			super.onExternalViewChange(externalViewList, changeContext);
			for(ClusterChangeListener listener:listeners){
			  listener.onChange();
			}
	    logger.info("End:Processing change in cluster topology");

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			lock.unlock();
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
	public InstanceConfig getDestination(String streamName, int partitionId) {
		List<InstanceConfig> instances = getInstances(streamName, streamName
				+ "_" + partitionId, "LEADER");
		if (instances.size() == 1) {
			return instances.get(0);
		} else {
			return null;
		}

	}

}
