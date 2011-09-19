package io.s4.comm.topology;

import io.s4.comm.topology.Cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Config {
    List<Cluster> clusters = new ArrayList<Cluster>();
    private String version = "-1";
    
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Config(String version) {
        this.version = version;
    }
    
    public Config() {
    }
    
    public void addCluster(Cluster cluster) {
        clusters.add(cluster);
    }

    public List<Cluster> getClusters() {
        return Collections.unmodifiableList(clusters);
    }
    
    public String toString() {
        return "{version="+version+",clusters="+clusters+"}";
    }
}
