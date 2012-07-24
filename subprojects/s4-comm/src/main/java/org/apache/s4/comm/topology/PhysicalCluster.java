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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * The S4 physical cluster implementation.
 * 
 */
public class PhysicalCluster {

    // TODO: do we need a Cluster interface to represent different types of
    // implementations?

    private static final Logger logger = LoggerFactory.getLogger(PhysicalCluster.class);

    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    String mode = "unicast";
    String name = "unknown";

    final private String[] hosts;
    final private String[] ports;
    final private int numNodes;
    private int numPartitions;

    public PhysicalCluster(int numPartitions) {
        this.hosts = new String[] {};
        this.ports = new String[] {};
        this.numNodes = 0;
        this.numPartitions = numPartitions;
    }

    /**
     * Define the hosts and corresponding ports in the cluster.
     * 
     * @param hosts
     *            a comma separates list of host names.
     * @param ports
     *            a comma separated list of ports.
     * @throws IOException
     *             if number of hosts and ports don't match.
     */
    PhysicalCluster(String hosts, String ports) throws IOException {
        if (hosts != null && hosts.length() > 0 && ports != null && ports.length() > 0) {
            this.ports = ports.split(",");
            this.hosts = hosts.split(",");

            if (this.ports.length != this.hosts.length) {
                logger.error("Number of hosts should match number of ports in properties file. hosts: " + hosts
                        + " ports: " + ports);
                throw new IOException();
            }

            numNodes = this.hosts.length;
            for (int i = 0; i < numNodes; i++) {
                ClusterNode node = new ClusterNode(i, Integer.parseInt(this.ports[i]), this.hosts[i], "");
                nodes.add(node);
                logger.info("Added cluster node: " + this.hosts[i] + ":" + this.ports[i]);
            }
            numPartitions = numNodes;
        } else {
            this.hosts = new String[] {};
            this.ports = new String[] {};
            this.numNodes = 0;

        }
    }

    /**
     * 
     * @return Number of partitions in the cluster.
     */
    public int getPartitionCount() {
        return numPartitions;
    }

    /**
     * @param node
     */
    public void addNode(ClusterNode node) {
        nodes.add(node);
    }

    /**
     * @return a list of {@link ClusterNode} objects available in the cluster.
     */
    public List<ClusterNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    // TODO: do we need mode and name? Making provate for now.

    @SuppressWarnings("unused")
    private String getMode() {
        return mode;
    }

    @SuppressWarnings("unused")
    private void setMode(String mode) {
        this.mode = mode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{ nbNodes=").append(nodes.size()).append(",name=").append(name).append(",mode=").append(mode)
                .append(",type=").append(",nodes=").append(nodes).append("}");
        return sb.toString();
    }

}
