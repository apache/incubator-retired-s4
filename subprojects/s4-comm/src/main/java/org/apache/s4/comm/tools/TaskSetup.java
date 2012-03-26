package org.apache.s4.comm.tools;

import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.TopologyFromZK;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;

public class TaskSetup {

    private ZkClient zkclient;

    public TaskSetup(String zookeeperAddress) {
        zkclient = new ZkClient(zookeeperAddress);
        zkclient.setZkSerializer(new ZNRecordSerializer());
        zkclient.waitUntilConnected();
    }

    public void clean(String clusterName) {
        zkclient.deleteRecursive("/" + clusterName);
    }

    public void setup(String clusterName, int tasks) {
        zkclient.createPersistent("/" + clusterName + "/tasks", true);
        zkclient.createPersistent("/" + clusterName + "/process", true);
        zkclient.createPersistent("/" + clusterName, true);
        for (int i = 0; i < tasks; i++) {
            String taskId = "Task-" + i;
            ZNRecord record = new ZNRecord(taskId);
            record.putSimpleField("taskId", taskId);
            record.putSimpleField("port", String.valueOf(1300 + i));
            record.putSimpleField("partition", String.valueOf(i));
            record.putSimpleField("cluster", clusterName);
            zkclient.createPersistent("/" + clusterName + "/tasks/" + taskId, record);
        }
    }

    public static void main(String[] args) throws Exception {
        TaskSetup taskSetup = new TaskSetup("localhost:2181");
        String clusterName = "test-s4-cluster";
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 10);
        String zookeeperAddress = "localhost:2181";
        for (int i = 0; i < 10; i++) {
            AssignmentFromZK assignmentFromZK = new AssignmentFromZK(clusterName, zookeeperAddress, 30000, 30000);
            ClusterNode assignClusterNode = assignmentFromZK.assignClusterNode();
            System.out.println(i + "-->" + assignClusterNode);
        }
        TopologyFromZK topologyFromZK = new TopologyFromZK(clusterName, zookeeperAddress, 30000, 30000);
        Thread.sleep(3000);
        Cluster topology = topologyFromZK.getTopology();
        System.out.println(topology.getNodes().size());
        Thread.currentThread().join();
    }
}
