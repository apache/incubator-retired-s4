package org.apache.s4.tools.yarn;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.Main.InlineConfigParameterConverter;

import com.beust.jcommander.Parameter;

public class CommonS4YarnArgs {

    public static final String NAMED_STRING_PARAMETERS = "-namedStringParameters";

    public static final String EXTRA_MODULES_CLASSES = "-extraModulesClasses";

    public static final String PRIORITY = "-priority";

    public static final String S4_NODE_MEMORY = "-s4NodeMemory";

    public static final String S4_NODE_JVM_PARAMETERS = "-s4NodeJVMParameters";

    public static final String NB_S4_NODES = "-nbS4Nodes";
    public static final String USER = "-user";

    @Parameter(names = { "-cluster", "-c" }, description = "Name of the S4 logical cluster that will contain S4 nodes. NOTE: the cluster is currently defined automatically in ZooKeeper.", required = true)
    String cluster;

    @Parameter(names = "-zk", description = "S4 Zookeeper cluster manager connection string", required = true)
    String zkString;

    @Parameter(names = { S4_NODE_MEMORY, "-container_memory" }, description = "YARN parameter: Amount of memory in MB to be requested to host the S4 node", required = false, validateWith = S4CLIYarnArgs.MemoryValidator.class)
    int containerMemory = 256;

    @Parameter(names = PRIORITY, description = "YARN parameter: Application priority", required = false)
    int priority = 0;

    @Parameter(names = USER, description = "YARN parameter: User to run the application as", required = false)
    String user = "";

    @Parameter(names = { NB_S4_NODES, "-num_containers" }, description = "YARN parameter: Number of containers on which the S4 node needs to be hosted (typically: at least as many partitions as the logical cluster)", validateWith = S4CLIYarnArgs.NbContainersValidator.class)
    int numContainers = 1;

    @Parameter(names = "-debug", description = "YARN parameter: Dump out debug information")
    boolean debug;

    @Parameter(names = "-test", description = "Test mode")
    boolean test;

    @Parameter(names = { EXTRA_MODULES_CLASSES, "-emc" }, description = "Comma-separated list of additional configuration modules (they will be instantiated through their constructor without arguments).", required = false, hidden = false)
    List<String> extraModulesClasses = new ArrayList<String>();

    @Parameter(names = { NAMED_STRING_PARAMETERS, "-p" }, description = "Comma-separated list of inline configuration parameters, taking precedence over homonymous configuration parameters from configuration files. Syntax: '-p=name1=value1,name2=value2 '", hidden = false, converter = InlineConfigParameterConverter.class)
    List<String> extraNamedParameters = new ArrayList<String>();

    // TODO parse JVM parameters that include commas
    @Parameter(names = S4CLIYarnArgs.S4_NODE_JVM_PARAMETERS, description = "Extra JVM parameter for running the nodes, specified as a comma separated list. The memory is usually configured through "
            + S4_NODE_MEMORY, required = false)
    List<String> extraS4NodeJVMParams = new ArrayList<String>();

}
