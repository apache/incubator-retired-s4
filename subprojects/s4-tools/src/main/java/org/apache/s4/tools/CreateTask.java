package org.apache.s4.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.tools.IdealStateCalculatorByShuffling;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class CreateTask extends S4ArgsBase
{
  public static void main(String[] args)
  {
    CreateTaskArgs taskArgs = new CreateTaskArgs();

    Tools.parseArgs(taskArgs, args);

    HelixAdmin admin = new ZKHelixAdmin(taskArgs.zkConnectionString);
    ConfigScopeBuilder builder = new ConfigScopeBuilder();
    ConfigScope scope = builder.forCluster(taskArgs.clusterName).forResource(taskArgs.taskId).build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("type", "Task");
    properties.put("streamName", taskArgs.streamName);
    properties.put("taskType", taskArgs.taskType);
    admin.setConfig(scope, properties);
    // A task is modeled as a resource in Helix
    admin.addResource(taskArgs.clusterName, taskArgs.taskId,
        taskArgs.numPartitions, "LeaderStandby",
        IdealStateModeProperty.AUTO.toString());
    // This does the assignment of partition to nodes. It uses a modified
    // version of consistent hashing to distribute active partitions and standbys
    // equally among nodes.
    admin.rebalance(taskArgs.clusterName, taskArgs.taskId, taskArgs.numStandBys + 1);
    
  }

  @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
  static class CreateTaskArgs extends S4ArgsBase
  {

    @Parameter(names = "-zk", description = "ZooKeeper connection string")
    String zkConnectionString = "localhost:2181";

    @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
    String clusterName;

    @Parameter(names={"-id","-taskId"},description = "id of the task that produces/consumes a stream", required = true, arity = 1)
    String taskId;

    @Parameter(names={"-t","-type"}, description = "producer/consumer", required = true, arity = 1)
    String taskType;

    @Parameter(names={"-p", "-partitions"},description = "Parallelism/Number of Partition for the task", required = true, arity = 1)
    Integer numPartitions;

    @Parameter(names={"-r", "standbys for each partition"},description = "Number of Standby processors for each active processor", required = false, arity = 1)
    Integer numStandBys = 1;

    @Parameter(names={"-s", "-streams"}, description = "name of the stream(s) it produces/consumes.", required = true, arity = 1)
    String streamName;

    @Parameter(names={"-n", "-nodes"},description = "Node ids where the stream processor must run on. Optional. By default, the processing is divided equally among all nodes.", required = false, arity = -1)
    List<String> nodeIds = Collections.emptyList();

  }
}
