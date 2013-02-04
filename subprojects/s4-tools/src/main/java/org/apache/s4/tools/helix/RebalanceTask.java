package org.apache.s4.tools.helix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
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
        IdealState currentAssignment = admin.getResourceIdealState(taskArgs.clusterName, taskArgs.taskId);
        List<String> instancesInGroup = new ArrayList<String>();
        List<String> instancesInCluster = admin.getInstancesInCluster(taskArgs.clusterName);
        for (String instanceName : instancesInCluster) {
            InstanceConfig instanceConfig = admin.getInstanceConfig(taskArgs.clusterName, instanceName);
            String nodeGroup = instanceConfig.getRecord().getSimpleField("GROUP");
            if (nodeGroup.equals(taskArgs.nodeGroup)) {
                instancesInGroup.add(instanceName);
            }
        }
        admin.rebalance(taskArgs.clusterName, currentAssignment, instancesInGroup);
    }

    @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
    static class RebalanceTaskArgs extends S4ArgsBase {

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = { "-id", "-taskId" }, description = "id of the task that produces/consumes a stream", required = true, arity = 1)
        String taskId;

        @Parameter(names = { "-ng", "-nodeGroup" }, description = "Node group name where the task needs to be run", required = true, arity = 1)
        String nodeGroup = "default";

    }
}
