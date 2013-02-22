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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.StatusUtils;
import org.apache.s4.tools.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class ClusterStatus extends S4ArgsBase {
    static Logger logger = LoggerFactory.getLogger(ClusterStatus.class);

    public static void main(String[] args) {

        StatusArgs statusArgs = new StatusArgs();
        Tools.parseArgs(statusArgs, args);

        try {
            if (statusArgs.clusters.size() > 0) {
                for (String cluster : statusArgs.clusters) {
                    HelixManager manager = HelixManagerFactory.getZKHelixManager(cluster, "ADMIN",
                            InstanceType.ADMINISTRATOR, statusArgs.zkConnectionString);
                    manager.connect();
                    ConfigAccessor configAccessor = manager.getConfigAccessor();
                    ConfigScopeBuilder builder = new ConfigScopeBuilder();
                    printClusterInfo(manager, cluster);
                    List<String> resourcesInCluster = manager.getClusterManagmentTool().getResourcesInCluster(cluster);

                    List<String> apps = new ArrayList<String>();
                    List<String> tasks = new ArrayList<String>();

                    for (String resource : resourcesInCluster) {
                        ConfigScope scope = builder.forCluster(cluster).forResource(resource).build();
                        String resourceType = configAccessor.get(scope, "type");
                        if ("App".equals(resourceType)) {
                            apps.add(resource);
                        } else if ("Task".equals(resourceType)) {
                            tasks.add(resource);
                        }
                    }
                    if (statusArgs.apps == null && statusArgs.streams == null) {
                        statusArgs.apps = apps;
                        statusArgs.streams = tasks;
                    }

                    System.out.println();
                    System.out.println(StatusUtils.title(" App Status ", '*', 130));

                    for (String app : statusArgs.apps) {
                        if (resourcesInCluster.contains(app)) {
                            printAppInfo(manager, cluster, app);
                        }
                    }

                    if (statusArgs.streams != null && statusArgs.streams.size() > 0) {
                        for (String stream : statusArgs.streams) {
                            if (resourcesInCluster.contains(stream)) {
                                printStreamInfo(manager, cluster, stream);
                            }
                        }
                    }
                    manager.disconnect();
                }
            }
        } catch (Exception e) {
            logger.error("Cannot get the status of S4", e);
        }

    }

    private static void printStreamInfo(HelixManager manager, String cluster, String taskId) {
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(cluster).forResource(taskId).build();
        String streamName = configAccessor.get(scope, "streamName");
        String taskType = configAccessor.get(scope, "taskType");

        System.out.println();
        System.out.println(StatusUtils.title(" Task Status ", '*', 130));
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", StatusUtils.inMiddle("Task Id", 20),
                StatusUtils.inMiddle("Cluster", 20), StatusUtils.inMiddle("Description", 90));
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-30s%-20s%-90s%n", StatusUtils.inMiddle(taskId, 30), StatusUtils.inMiddle(cluster, 20),
                StatusUtils.inMiddle(streamName + " " + ((taskType == null) ? "(untyped)" : taskType), 90));
        System.out.println(StatusUtils.generateEdge(130));
        HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = helixDataAccessor.keyBuilder();
        IdealState assignment = helixDataAccessor.getProperty(keyBuilder.idealStates(taskId));
        ExternalView view = helixDataAccessor.getProperty(keyBuilder.externalView(taskId));
        if (view == null) {
            System.out.println(StatusUtils.noInfo("INFORMATION NOT AVAILABLE FOR TASK [" + taskId + "]"));
            return;
        }
        List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder.liveInstances());
        System.out.format("%-30s%-100s%n", StatusUtils.inMiddle("Partition", 30), StatusUtils.inMiddle("State", 100));
        System.out.println(StatusUtils.generateEdge(130));
        for (String partition : assignment.getPartitionSet()) {
            Map<String, String> stateMap = view.getStateMap(partition);
            if (stateMap == null) {
                System.out.println(StatusUtils.noInfo("INFORMATION NOT AVAILABLE FOR TASK [" + taskId
                        + "] / PARTITION [" + partition + "]"));
                return;
            }
            StringBuilder sb = new StringBuilder();
            String delim = "";
            for (String instance : stateMap.keySet()) {
                sb.append(delim);
                String state = stateMap.get(instance);
                if (liveInstances.contains(instance)) {
                    sb.append(instance).append(":").append(state);
                } else {
                    sb.append(instance).append(":").append("OFFLINE");
                }
                delim = ", ";
            }
            System.out.format("%-50s%-10s%n", StatusUtils.inMiddle(partition, 50),
                    StatusUtils.inMiddle(sb.toString(), 100));
        }
        System.out.println(StatusUtils.generateEdge(130));
        System.out.println("\n\n");

    }

    private static void printAppInfo(HelixManager manager, String cluster, String app) {
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(cluster).forResource(app).build();
        String uri = configAccessor.get(scope, AppConfig.APP_URI);

        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", StatusUtils.inMiddle("Name", 20), StatusUtils.inMiddle("Cluster", 20),
                StatusUtils.inMiddle("URI", 90));
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", StatusUtils.inMiddle(app, 20), StatusUtils.inMiddle(cluster, 20),
                StatusUtils.inMiddle(uri, 90));
        System.out.println(StatusUtils.generateEdge(130));
        HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = helixDataAccessor.keyBuilder();
        IdealState assignment = helixDataAccessor.getProperty(keyBuilder.idealStates(app));
        ExternalView view = helixDataAccessor.getProperty(keyBuilder.externalView(app));
        if (view == null) {
            System.out.println(StatusUtils.noInfo("INFORMATION NOT AVAILABLE FOR APP [" + app + "]"));
            return;
        }
        List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder.liveInstances());
        Map<String, String> assignmentMap = assignment.getInstanceStateMap(app);
        Map<String, String> appStateMap = view.getStateMap(app);
        if (appStateMap == null) {
            System.out.println(StatusUtils.noInfo("INFORMATION NOT AVAILABLE FOR APP [" + app + "]"));
            return;
        }
        System.out.format("%-50s%-20s%n", StatusUtils.inMiddle("Node id", 50), StatusUtils.inMiddle("DEPLOYED", 20));
        System.out.println(StatusUtils.generateEdge(130));
        for (String instance : assignmentMap.keySet()) {
            String state = appStateMap.get(instance);
            System.out.format("%-50s%-10s%n", StatusUtils.inMiddle(instance, 50), StatusUtils.inMiddle(
                    (("ONLINE".equals(state) && liveInstances.contains(instance)) ? "Y" : "N"), 20));
        }

        System.out.println(StatusUtils.generateEdge(130));
        System.out.println("\n\n");

    }

    private static void printClusterInfo(HelixManager manager, String cluster) {
        HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = dataAccessor.keyBuilder();
        List<String> instances = dataAccessor.getChildNames(keyBuilder.instanceConfigs());
        List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
        if (liveInstances == null) {
            liveInstances = Collections.emptyList();
        }
        System.out.println();
        System.out.println(StatusUtils.title(" Cluster Status ", '*', 130));
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-50s%-80s%n", " ", StatusUtils.inMiddle("Nodes", 80));
        System.out.format("%-20s%-20s%-10s%s%n", StatusUtils.inMiddle("Cluster Name", 20),
                StatusUtils.inMiddle("Nodes", 20), StatusUtils.inMiddle("Active", 10), StatusUtils.generateEdge(80));
        System.out.format("%-54s%-10s%-50s%-8s%-8s%n", " ", StatusUtils.inMiddle("Node id", 10),
                StatusUtils.inMiddle("Host", 50), StatusUtils.inMiddle("Port", 8), StatusUtils.inMiddle("Active", 10));
        System.out.println(StatusUtils.generateEdge(130));

        System.out.format("%-20s%-20s%-10s", StatusUtils.inMiddle(cluster, 20),
                StatusUtils.inMiddle("" + instances.size(), 8), StatusUtils.inMiddle("" + liveInstances.size(), 8));
        boolean first = true;

        for (String instance : instances) {
            InstanceConfig config = dataAccessor.getProperty(keyBuilder.instanceConfig(instance));
            // System.out.println(config);
            if (first) {
                first = false;
            } else {
                System.out.format("%n%-50s", " ");
            }
            System.out.format("%-30s%-46s%-10s%-10s", StatusUtils.inMiddle("" + config.getId(), 30),
                    StatusUtils.inMiddle(config.getHostName(), 50), StatusUtils.inMiddle(config.getPort() + "", 10),
                    StatusUtils.inMiddle(liveInstances.contains(config.getInstanceName()) ? "Y" : "N", 10));
        }

        System.out.println("\n\n");
    }

    @Parameters(commandNames = "s4 status", commandDescription = "Show status of S4", separators = "=")
    static class StatusArgs extends HelixS4ArgsBase {

        @Parameter(names = { "-app" }, description = "Only show status of specified S4 application(s)", required = false)
        List<String> apps;

        @Parameter(names = { "-c", "-cluster" }, description = "Only show status of specified S4 cluster(s)", required = true)
        List<String> clusters;

        @Parameter(names = { "-s", "-stream" }, description = "Only show status of specified published stream(s)", required = false)
        List<String> streams;

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = "-timeout", description = "Connection timeout to Zookeeper, in ms")
        int timeout = 10000;
    }

}
