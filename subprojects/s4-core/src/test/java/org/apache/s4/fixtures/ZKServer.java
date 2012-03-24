package org.apache.s4.fixtures;

import org.apache.s4.comm.tools.TaskSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class ZKServer {

    private static Logger logger = LoggerFactory.getLogger(ZKServer.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        ZKServerArgs clusterArgs = new ZKServerArgs();
        JCommander jc = new JCommander(clusterArgs);
        try {
            jc.parse(args);
        } catch (Exception e) {
            jc.usage();
            System.exit(-1);
        }
        try {

            logger.info("Starting zookeeper server for cluster [{}] with [{}] node(s)", clusterArgs.clusterName,
                    clusterArgs.nbTasks);
            CommTestUtils.startZookeeperServer();
            TaskSetup taskSetup = new TaskSetup(clusterArgs.zkConnectionString);
            taskSetup.clean(clusterArgs.clusterName);
            taskSetup.setup(clusterArgs.clusterName, clusterArgs.nbTasks);
            logger.info("Zookeeper started");
        } catch (Exception e) {
            logger.error("Cannot initialize zookeeper with specified configuration", e);
        }

    }

    @Parameters(separators = "=", commandDescription = "Start Zookeeper server and initialize S4 cluster configuration in Zookeeper (and clean previous one with same cluster name)")
    static class ZKServerArgs {

        @Parameter(names = "cluster", description = "S4 cluster name", required = true)
        String clusterName = "s4-test-cluster";

        @Parameter(names = "nbTasks", description = "number of tasks for the cluster", required = true)
        int nbTasks = 1;

        @Parameter(names = "zk", description = "Zookeeper connection string")
        String zkConnectionString = "localhost:2181";
    }

}
