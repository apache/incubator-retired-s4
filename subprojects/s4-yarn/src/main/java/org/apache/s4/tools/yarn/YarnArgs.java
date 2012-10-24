package org.apache.s4.tools.yarn;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = "yarn", separators = "=", commandDescription = "Deploy S4 application on YARN")
class YarnArgs {

    @Parameter(names = "-appName", description = "Name of the application", required = false)
    String appName = "S4App";

    @Parameter(names = "-priority", description = "Application priority", required = false)
    int priority = 0;

    @Parameter(names = "-queue", description = "RM Queue in which this application is to be submitted", required = false)
    String queue = "default";

    @Parameter(names = "-user", description = "User to run the application as", required = false)
    String user = "";

    @Parameter(names = "-timeout", description = "Application timeout in milliseconds (default is: -1 = no timeout)", required = false)
    int timeout = -1;

    @Parameter(names = "-s4Dir", description = "S4 directory. It is used to resolve S4 platform libraries and dependencies, currently from s4dir/subprojects/s4-yarn/build/install/s4-yarn/lib", required = true, hidden = true)
    String s4Dir;

    public static class MemoryValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) throws ParameterException {
            if (Integer.valueOf(value).intValue() < 0) {
                throw new ParameterException("Invalid memory size: " + value);
            }
        }
    }

    @Parameter(names = "-master_memory", description = "Amount of memory in MB to be requested to run the application master", required = false, validateWith = YarnArgs.MemoryValidator.class)
    int masterMemory = 256;

    @Parameter(names = { "-cluster", "-c" }, description = "Name of the S4 logical cluster that will contain S4 nodes. NOTE: the cluster is currently defined automatically in ZooKeeper.", required = true)
    String cluster;

    @Parameter(names = { "-nbTasks", "-nbPartitions" }, description = "Number of partitions of the S4 cluster", required = true)
    int nbTasks;

    @Parameter(names = { "-flp", "-firstListeningPort" }, description = "First listening port for S4 nodes", required = true)
    int flp = 12000;

    @Parameter(names = "-s4r", description = "URI to S4 archive (.s4r) file. It will be automatically deployed on the allocated S4 nodes. Examples: file:///home/s4/file.s4r or more probably hdfs:///hostname:port/path/file.s4r", required = true)
    String s4rPath;

    @Parameter(names = "-container_memory", description = "Amount of memory in MB to be requested to host the S4 node", required = false, validateWith = YarnArgs.MemoryValidator.class)
    int containerMemory = 10;

    public static class NbContainersValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (Integer.valueOf(value).intValue() < 1) {
                throw new ParameterException("Invalid number of containers: " + value);
            }
        }
    }

    @Parameter(names = "-num_containers", description = "Number of containers on which the S4 node needs to be hosted (typically: at least as many partitions as the logical cluster)", validateWith = YarnArgs.NbContainersValidator.class)
    int numContainers = 1;

    @Parameter(names = "-zk", description = "S4 Zookeeper cluster manager connection string", required = true)
    String zkString;

    @Parameter(names = "-log_properties", description = "log4j.properties file", required = false)
    String logProperties = "";

    @Parameter(names = "-debug", description = "Dump out debug information")
    boolean debug;

}
