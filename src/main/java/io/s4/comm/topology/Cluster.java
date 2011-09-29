package io.s4.comm.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Cluster {
    public enum ClusterType {
        S4("s4"),
        ADAPTER("adapter");
        
        private final String clusterTypeString;
        
        private ClusterType(String eventShortName){
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
        sb.append("{name=").append(name).
            append(",mode=").append(mode).
            append(",type=").append(type).
            append(",nodes=").append(nodes).append("}");
        return sb.toString();
    }
    
}