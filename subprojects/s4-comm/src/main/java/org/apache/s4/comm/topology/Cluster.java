package org.apache.s4.comm.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * 
 * The S4 physical cluster implementation.
 * 
 */
@Singleton
public class Cluster {

    // TODO: do we need a Cluster interface to represent different types of
    // implementations?

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    // List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    HashBiMap<Integer, ClusterNode> nodes = HashBiMap.create();
    String mode = "unicast";
    String name = "unknown";

    final private String[] hosts;
    final private String[] ports;
    final private int numNodes;
    private int numPartitions;

    public Cluster(int numPartitions) {
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
    @Inject
    Cluster(@Named("cluster.hosts") String hosts, @Named("cluster.ports") String ports) throws IOException {
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
                addNode(node);
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
     * Number of partitions in the cluster.
     * 
     * @return
     */
    public int getPartitionCount() {
        return numPartitions;
    }

    /**
     * @param node
     */
    public void addNode(ClusterNode node) {
        // nodes.add(node);
        if (nodes.containsKey(node.getPartition())) {
            logger.debug("Partition {} already has a corresponding ClusterNode", node.getPartition());
        }
        if (nodes.containsValue(node)) {
            logger.error("ClusterNode is already mapped to partition {}", nodes.inverse().get(node));
        }
        nodes.forcePut(node.getPartition(), node);
    }

    /**
     * @return a list of {@link ClusterNode} objects available in the cluster.
     */
    public Set<ClusterNode> getNodes() {
        return Collections.unmodifiableSet(nodes.values());
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

    @SuppressWarnings("unused")
    private String getName() {
        return name;
    }

    @SuppressWarnings("unused")
    private void setName(String name) {
        this.name = name;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{name=").append(name).append(",mode=").append(mode).append(",type=").append(",nodes=").append(nodes)
                .append("}");
        return sb.toString();
    }

}
