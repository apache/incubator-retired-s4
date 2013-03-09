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

import org.apache.s4.comm.tools.TaskSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Defines an S4 cluster in the cluster manager.
 */
public class DefineCluster {

    static Logger logger = LoggerFactory.getLogger(DefineCluster.class);

    public static void main(String[] args) {

        ZKServerArgs clusterArgs = new ZKServerArgs();
        Tools.parseArgs(clusterArgs, args);
        try {

            logger.info("preparing new cluster [{}] with [{}] node(s)", clusterArgs.clusterName, clusterArgs.nbTasks);

            TaskSetup taskSetup = new TaskSetup(clusterArgs.zkConnectionString);
            taskSetup.clean(clusterArgs.clusterName);
            taskSetup.setup(clusterArgs.clusterName, clusterArgs.nbTasks, clusterArgs.firstListeningPort);
            logger.info("New cluster configuration uploaded into zookeeper");
        } catch (Exception e) {
            logger.error("Cannot initialize zookeeper with specified configuration", e);
        }

    }

    @Parameters(commandNames = "s4 newCluster", separators = "=", commandDescription = "Setup new S4 logical cluster")
    static class ZKServerArgs extends S4ArgsBase {

        @Parameter(names = { "-c", "-cluster" }, description = "S4 cluster name", required = true)
        String clusterName = "s4-test-cluster";

        @Parameter(names = "-nbTasks", description = "number of tasks for the cluster", required = true)
        int nbTasks = 1;

        @Parameter(names = "-zk", description = "Zookeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-flp", "-firstListeningPort" }, description = "Initial listening port for nodes in this cluster. First node listens on the specified port, other nodes listen on port initial + nodeIndex", required = true)
        int firstListeningPort = -1;
    }

}
