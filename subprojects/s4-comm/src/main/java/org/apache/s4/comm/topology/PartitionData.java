package org.apache.s4.comm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class PartitionData {
    int numPartitions;
    boolean isExclusive = false;
    Map<Integer, Integer> globalPartitionMap = Maps.newHashMap();
    List<String> streams = new ArrayList<String>();

    public PartitionData() {

    }

    public PartitionData(boolean isExclusive, int numPartitions) {
        this.numPartitions = numPartitions;
        this.isExclusive = isExclusive;
    }

    public boolean isExclusive() {
        return this.isExclusive;
    }

    public int getPartitionCount() {
        return this.numPartitions;
    }

    public void addPartitionMappingInfo(int partitionId, int nodeId) {
        globalPartitionMap.put(partitionId, nodeId);
    }

    public List<String> getStreams() {
        return streams;
    }

    public void addStream(String stream) {
        if (!streams.contains(stream)) {
            streams.add(stream);
            System.out.println("Add " + stream);
        }
    }

    public void setStreams(List<String> streams) {
        this.streams = streams;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setPartitionCount(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public void setExclusive(boolean isExclusive) {
        this.isExclusive = isExclusive;
    }

    public int getGlobalePartitionId(int partitionId) {
        return globalPartitionMap.get(partitionId);
    }

    public String toString() {
        return "PartitionCount: " + numPartitions + ", isExclusive: " + isExclusive + ", partitionMap: "
                + globalPartitionMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((globalPartitionMap == null) ? 0 : globalPartitionMap.hashCode());
        result = prime * result + (isExclusive ? 1231 : 1237);
        result = prime * result + numPartitions;
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
        PartitionData other = (PartitionData) obj;
        if (globalPartitionMap == null) {
            if (other.globalPartitionMap != null)
                return false;
        } else if (!globalPartitionMap.equals(other.globalPartitionMap))
            return false;
        if (isExclusive != other.isExclusive)
            return false;
        if (numPartitions != other.numPartitions)
            return false;
        return true;
    }

}
