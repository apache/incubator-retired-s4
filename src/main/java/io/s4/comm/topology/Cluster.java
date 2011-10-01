package io.s4.comm.topology;

import io.s4.comm.netty.NettyEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.tools.corba.se.idl.InvalidArgument;

@Singleton
public class Cluster {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    public enum ClusterType {
        S4("s4"), ADAPTER("adapter");

        private final String clusterTypeString;

        private ClusterType(String eventShortName) {
            this.clusterTypeString = eventShortName;
        }

        public String toString() {
            return clusterTypeString;
        }
    }

    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    String mode = "unicast";
    String name = "unknown";

    ClusterType type = ClusterType.S4;

    Cluster() {
        this.ports = null;
        this.hosts = null;
        this.numNodes = 0;
    }

    final private String[] hosts;
    final private String[] ports;
    final private int numNodes;

    @Inject
    Cluster(@Named("cluster.hosts") String hosts,
            @Named("cluster.ports") String ports) throws IOException {

        this.ports = ports.split(",");
        this.hosts = hosts.split(",");

        if (this.ports.length != this.hosts.length) {
            logger.error("Number of hosts should match number of ports in properties file. hosts: "
                    + hosts + " ports: " + ports);
            throw new IOException();
        }
        
        numNodes = this.hosts.length;
        for(int i=0; i<numNodes; i++) {
            ClusterNode node = new ClusterNode(i, Integer.parseInt(this.ports[i]), this.hosts[i], "");
            nodes.add(node);
            logger.trace("Added cluster node: " + this.hosts[i] + ":" + this.ports[i]);
        }
    }

    public void addNode(ClusterNode node) {
        nodes.add(node);
    }

    public List<ClusterNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ClusterType getType() {
        return type;
    }

    public void setType(ClusterType type) {
        this.type = type;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{name=").append(name).append(",mode=").append(mode)
                .append(",type=").append(type).append(",nodes=").append(nodes)
                .append("}");
        return sb.toString();
    }

}