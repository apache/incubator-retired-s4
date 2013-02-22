package org.apache.s4.tools.helix;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.s4.comm.helix.S4HelixConstants;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.Tools;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class RebalanceTask extends S4ArgsBase {
    public static void main(String[] args) {
        RebalanceTaskArgs taskArgs = new RebalanceTaskArgs();

        Tools.parseArgs(taskArgs, args);

        HelixAdmin admin = new ZKHelixAdmin(taskArgs.zkConnectionString);
        // This does the assignment of partition to nodes. It uses a modified
        // version of consistent hashing to distribute active partitions and standbys
        // equally among nodes.
        IdealState currentAssignment = admin.getResourceIdealState(S4HelixConstants.HELIX_CLUSTER_NAME, taskArgs.taskId);
        List<String> instancesInGroup = new ArrayList<String>();
        List<String> instancesInCluster = admin.getInstancesInCluster(S4HelixConstants.HELIX_CLUSTER_NAME);
        for (String instanceName : instancesInCluster) {
            InstanceConfig instanceConfig = admin.getInstanceConfig(S4HelixConstants.HELIX_CLUSTER_NAME, instanceName);
            String nodeGroup = instanceConfig.getRecord().getSimpleField("GROUP");
            if (nodeGroup.equals(taskArgs.clusterName)) {
                instancesInGroup.add(instanceName);
            }
        }
        admin.rebalance(S4HelixConstants.HELIX_CLUSTER_NAME, currentAssignment, instancesInGroup);
    }

    @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
    static class RebalanceTaskArgs extends HelixS4ArgsBase {

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = { "-id", "-taskId" }, description = "id of the task that produces/consumes a stream", required = true, arity = 1)
        String taskId;

    }
}
