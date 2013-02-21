package org.apache.s4.tools.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class RemoveTask extends S4ArgsBase {

    static Logger logger = LoggerFactory.getLogger(CreateTask.class);

    public static void main(String[] args) {
        RemoveTaskArgs taskArgs = new RemoveTaskArgs();

        Tools.parseArgs(taskArgs, args);
        String msg = String.format("Removing task [{}] from cluster [{}]", taskArgs.taskId, taskArgs.clusterName);
        logger.info(msg);
        HelixAdmin admin = new ZKHelixAdmin(taskArgs.zkConnectionString);
        admin.dropResource(taskArgs.clusterName, taskArgs.taskId);
        logger.info("Finished Removing task:" + taskArgs.taskId + " from cluster:" + taskArgs.clusterName);
    }

    @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
    static class RemoveTaskArgs extends HelixS4ArgsBase {

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = { "-id", "-taskId" }, description = "id of the task that produces/consumes a stream", required = true, arity = 1)
        String taskId;

    }

}
