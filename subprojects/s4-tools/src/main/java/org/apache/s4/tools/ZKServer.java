/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.s4.comm.tools.TaskSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Utility for running a simple ZooKeeper server instance. Useful for testing and prototyping but not adequate for
 * integration/production deployments.
 */
public class ZKServer {

    private static final String TEST_MODE_CLUSTER_CONF_2 = "c=testCluster2:flp=13000:nbTasks=1";
    private static final String TEST_MODE_CLUSTER_CONF_1 = "c=testCluster1:flp=12000:nbTasks=1";
    static Logger logger = LoggerFactory.getLogger(ZKServer.class);

    public static void main(String[] args) {

        ZKServerArgs zkArgs = new ZKServerArgs();
        Tools.parseArgs(zkArgs, args);
        try {

            logger.info("Starting zookeeper server on port [{}]", zkArgs.zkPort);

            if (zkArgs.clean) {
                logger.info("cleaning existing data in [{}] and [{}]", zkArgs.dataDir, zkArgs.logDir);
                FileUtils.deleteDirectory(new File(zkArgs.dataDir));
                FileUtils.deleteDirectory(new File(zkArgs.logDir));
            }
            IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

                @Override
                public void createDefaultNameSpace(ZkClient zkClient) {

                }
            };

            ZkServer zkServer = new ZkServer(zkArgs.dataDir, zkArgs.logDir, defaultNameSpace, zkArgs.zkPort);
            zkServer.start();

            logger.info("Zookeeper server started on port [{}]", zkArgs.zkPort);

            // now upload cluster configs if specified or if using test mode
            List<ClusterConfig> clusterConfigs = zkArgs.clusterConfigs;
            if (clusterConfigs == null) {
                if (zkArgs.testMode) {
                    logger.info("Initializing test mode with default clusters configurations");
                    clusterConfigs = new ArrayList<ZKServer.ClusterConfig>() {
                        {
                            add(new ClusterConfig(TEST_MODE_CLUSTER_CONF_1));
                            add(new ClusterConfig(TEST_MODE_CLUSTER_CONF_2));
                        }
                    };
                } else {
                    clusterConfigs = Collections.emptyList();
                }
            }
            for (ClusterConfig config : clusterConfigs) {
                TaskSetup taskSetup = new TaskSetup("localhost:" + zkArgs.zkPort);
                taskSetup.clean(config.clusterName);
                taskSetup.setup(config.clusterName, config.nbTasks, config.firstListeningPort);
                logger.info("Defined S4 cluster [{}] with [{}] tasks with first listening port [{}]", new String[] {
                        config.clusterName, String.valueOf(config.nbTasks), String.valueOf(config.firstListeningPort) });
            }

        } catch (Exception e) {
            logger.error("Cannot initialize zookeeper with specified configuration", e);
        }
    }

    @Parameters(commandNames = "s4 zkServer", separators = "=", commandDescription = "Start Zookeeper server")
    static class ZKServerArgs extends S4ArgsBase {

        @Parameter(names = "-port", description = "Zookeeper port")
        int zkPort = 2181;

        @Parameter(names = "-dataDir", description = "data directory", required = false)
        String dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "data").getAbsolutePath();

        @Parameter(names = "-clean", description = "clean zookeeper data (make sure you specify correct directories...)")
        boolean clean = false;

        @Parameter(names = "-logDir", description = "log directory")
        String logDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "log").getAbsolutePath();

        @Parameter(names = { "-t", "-testMode" }, description = "Launch Zookeeper instance and load a default cluster configuration for easy testing (2 clusters with following configs: "
                + TEST_MODE_CLUSTER_CONF_1 + " and " + TEST_MODE_CLUSTER_CONF_2 + "")
        boolean testMode = false;

        @Parameter(names = "-clusters", description = "Inline clusters configuration, comma-separated list of cluster definitions with format: c=<cluster name>:flp=<first listening port for this cluster>:nbTasks=<number of tasks> (Overrides default configuration for test mode)", converter = ClusterConfigsConverter.class)
        List<ClusterConfig> clusterConfigs;

    }

    public static class ClusterConfigsConverter implements IStringConverter<ClusterConfig> {

        @Override
        public ClusterConfig convert(String arg) {
            Pattern clusterConfigPattern = Pattern.compile("(c=\\w+[:]flp=\\d+[:]nbTasks=\\d+)");
            logger.info("processing cluster configuration {}", arg);
            Matcher configMatcher = clusterConfigPattern.matcher(arg);
            if (!configMatcher.find()) {
                throw new IllegalArgumentException("Cannot understand parameter " + arg);
            }
            String clusterConfigString = configMatcher.group(1);
            return new ClusterConfig(clusterConfigString);
        }
    }

    public static class ClusterConfig {

        public ClusterConfig(String config) {
            String[] split = config.split(":");
            this.clusterName = split[0].split("=")[1];
            this.firstListeningPort = Integer.valueOf(split[1].split("=")[1]);
            this.nbTasks = Integer.valueOf(split[2].split("=")[1]);

        }

        String clusterName;
        int firstListeningPort;
        int nbTasks;

    }

}
