package org.apache.s4.comm.topology;

public class ClusterNode {
    private int partition;
    private int port;
    private String machineName;
    private String taskId;
    
    public ClusterNode(int partition, int port, String machineName, String taskId) {
        this.partition = partition;
        this.port = port;
        this.machineName = machineName;
        this.taskId = taskId;
    }
    
    public int getPartition() {
        return partition;
    }
    public int getPort() {
        return port;
    }
    public String getMachineName() {
        return machineName;
    }
    public String getTaskId() {
        return taskId;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{").append("partition=").append(partition).
            append(",port=").append(port).
            append(",machineName=").append(machineName).
            append(",taskId=").append(taskId).append("}");
        return sb.toString();
    }
}
