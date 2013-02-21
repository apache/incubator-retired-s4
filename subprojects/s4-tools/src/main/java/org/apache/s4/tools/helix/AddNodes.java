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

package org.apache.s4.tools.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.s4.tools.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class AddNodes {

    static Logger logger = LoggerFactory.getLogger(AddNodes.class);

    public static void main(String[] args) {

        ZKServerArgs clusterArgs = new ZKServerArgs();
        Tools.parseArgs(clusterArgs, args);
        try {

            logger.info("Adding new nodes [{}] to cluster [{}] node(s)", clusterArgs.nbNodes, clusterArgs.clusterName);
            HelixAdmin helixAdmin = new ZKHelixAdmin(clusterArgs.zkConnectionString);
            int initialPort = clusterArgs.firstListeningPort;
            if (clusterArgs.nbNodes > 0) {
                String[] split = clusterArgs.nodes.split(",");
                for (int i = 0; i < clusterArgs.nbNodes; i++) {
                    String host = "localhost";
                    if (split.length > 0 && split.length == clusterArgs.nbNodes) {
                        host = split[i].trim();
                    }
                    InstanceConfig instanceConfig = new InstanceConfig("node_" + host + "_" + initialPort);
                    instanceConfig.setHostName(host);
                    instanceConfig.setPort("" + initialPort);
                    instanceConfig.getRecord().setSimpleField("GROUP", clusterArgs.nodeGroup);
                    helixAdmin.addInstance(clusterArgs.clusterName, instanceConfig);
                    initialPort = initialPort + 1;
                }
            }
            logger.info("New nodes configuration uploaded into zookeeper");
        } catch (Exception e) {
            logger.error("Cannot initialize zookeeper with specified configuration", e);
        }

    }

    @Parameters(commandNames = "s4 addNodes", separators = "=", commandDescription = "Setup new S4 logical cluster")
    static class ZKServerArgs extends HelixS4ArgsBase {

        @Parameter(names = { "-c", "-cluster" }, description = "S4 cluster name", required = true)
        String clusterName = "s4-test-cluster";

        @Parameter(names = "-nbNodes", description = "number of S4 nodes for the cluster", required = true)
        int nbNodes = 1;

        @Parameter(names = "-nodes", description = "Host names of the nodes", required = false)
        String nodes = "";

        @Parameter(names = "-zk", description = "Zookeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-flp", "-firstListeningPort" }, description = "Initial listening port for nodes in this cluster. First node listens on the specified port, other nodes listen on port initial + nodeIndex", required = true)
        int firstListeningPort = -1;

        @Parameter(names = { "-ng", "-nodeGroup" }, description = "Assign the nodes to one or more groups. This will be useful when you create task", required = false)
        String nodeGroup = "default";

    }

}
