package org.apache.s4.comm.udp;

import org.apache.s4.base.Destination;
import org.apache.s4.comm.topology.ClusterNode;

public class UDPDestination extends ClusterNode implements Destination {

    public UDPDestination(ClusterNode clusterNode) {
        this(clusterNode.getPartition(), clusterNode.getPort(), clusterNode.getMachineName(), clusterNode.getTaskId());
    }

    public UDPDestination(int partition, int port, String machineName, String taskId) {
        super(partition, port, machineName, taskId);
        // TODO Auto-generated constructor stub
    }

}
