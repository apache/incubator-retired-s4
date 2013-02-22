package org.apache.s4.tools.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.s4.comm.helix.S4HelixConstants;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class CreateTask extends S4ArgsBase {

    static Logger logger = LoggerFactory.getLogger(CreateTask.class);

    public static void main(String[] args) {
        CreateTaskArgs taskArgs = new CreateTaskArgs();

        Tools.parseArgs(taskArgs, args);
        String msg = String.format("Setting up new pe [{}] for stream(s) on nodes belonging to node group {}",
                taskArgs.taskId, taskArgs.streamName, taskArgs.clusterName);
        logger.info(msg);
        HelixAdmin admin = new ZKHelixAdmin(taskArgs.zkConnectionString);
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(S4HelixConstants.HELIX_CLUSTER_NAME).forResource(taskArgs.taskId).build();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("GROUP", taskArgs.clusterName);
        properties.put("type", "Task");
        properties.put("streamName", taskArgs.streamName);
        admin.setConfig(scope, properties);
        // A task is modeled as a resource in Helix
        admin.addResource(S4HelixConstants.HELIX_CLUSTER_NAME, taskArgs.taskId, taskArgs.numPartitions, "LeaderStandby",
                IdealStateModeProperty.AUTO.toString());
        // This does the assignment of partition to nodes. It uses a modified
        // version of consistent hashing to distribute active partitions and standbys
        // equally among nodes.
        List<String> instancesInGroup = new ArrayList<String>();
        List<String> instancesInCluster = admin.getInstancesInCluster(S4HelixConstants.HELIX_CLUSTER_NAME);
        for (String instanceName : instancesInCluster) {
            InstanceConfig instanceConfig = admin.getInstanceConfig(S4HelixConstants.HELIX_CLUSTER_NAME, instanceName);
            String nodeGroup = instanceConfig.getRecord().getSimpleField("GROUP");
            if (nodeGroup.equals(taskArgs.clusterName)) {
                instancesInGroup.add(instanceName);
            }
        }
        admin.rebalance(S4HelixConstants.HELIX_CLUSTER_NAME, taskArgs.taskId, taskArgs.numStandBys + 1, instancesInGroup);
        logger.info("Finished setting up task:" + taskArgs.taskId + " on nodes " + instancesInGroup);
    }

    @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
    static class CreateTaskArgs extends HelixS4ArgsBase {

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = { "-id", "-taskName" }, description = "name of the Task. Must be unique", required = true, arity = 1)
        String taskId;

        @Parameter(names = { "-p", "-partitions" }, description = "Parallelism/Number of Partition for the task", required = true, arity = 1)
        Integer numPartitions;

        @Parameter(names = { "-r", "standbys for each partition" }, description = "Number of Standby processors for each active processor", required = false, arity = 1)
        Integer numStandBys = 1;

        @Parameter(names = { "-s", "-stream" }, description = "name of the stream the pe listens to", required = true, arity = 1)
        String streamName;

    }
}
