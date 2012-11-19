package org.apache.s4.tools.yarn;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
class S4CLIYarnArgs extends CommonS4YarnArgs {

    public static final String S4_YARN_MASTER_MEMORY = "-s4YarnMasterMemory";

    public static final String QUEUE = "-queue";

    public static final String S4_DIR = "-s4Dir";

    @Parameter(names = "-appName", description = "Name of the application", required = false)
    String appName = "S4App";

    @Parameter(names = { "-nbTasks", "-nbPartitions" }, description = "Number of partitions of the S4 cluster", required = true)
    int nbTasks;

    @Parameter(names = { "-flp", "-firstListeningPort" }, description = "First listening port for S4 nodes", required = true)
    int flp = 12000;

    @Parameter(names = "-s4r", description = "URI to S4 archive (.s4r) file. It will be automatically deployed on the allocated S4 nodes. Examples: file:///home/s4/file.s4r or more probably hdfs:///hostname:port/path/file.s4r", required = true)
    String s4rPath;

    @Parameter(names = S4_DIR, description = "S4 directory. It is used to resolve S4 platform libraries and dependencies, currently from s4dir/subprojects/s4-yarn/build/install/s4-yarn/lib", required = true, hidden = true)
    String s4Dir;

    @Parameter(names = QUEUE, description = "YARN parameter:  RM Queue in which this application is to be submitted", required = false)
    String queue = "default";

    @Parameter(names = "-timeout", description = "YARN parameter: Application timeout in milliseconds (default is: -1 = no timeout)", required = false)
    int timeout = -1;

    @Parameter(names = { "-master_memory", S4_YARN_MASTER_MEMORY }, description = "YARN parameter: Amount of memory in MB to be requested to run the application master", required = false, validateWith = S4CLIYarnArgs.MemoryValidator.class)
    int masterMemory = 256;

    @Parameter(names = "-log_properties", description = "YARN parameter: log4j.properties file", required = false)
    String logProperties = "";

    public static class MemoryValidator implements IParameterValidator {

        @Override
        public void validate(String name, String value) throws ParameterException {
            if (Integer.valueOf(value).intValue() < 0) {
                throw new ParameterException("Invalid memory size: " + value);
            }
        }
    }

    public static class NbContainersValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (Integer.valueOf(value).intValue() < 1) {
                throw new ParameterException("Invalid number of containers: " + value);
            }
        }
    }

}
