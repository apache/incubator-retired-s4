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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.Tools;
import org.apache.s4.tools.S4ArgsBase.GradleOptsConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Maps;

public class S4Status extends S4ArgsBase {
    static Logger logger = LoggerFactory.getLogger(S4Status.class);

    private static String NONE = "--";

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

        System.out.println("Task Status");
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", inMiddle("Task Id", 20), inMiddle("Cluster", 20),
                inMiddle("Description", 90));
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", inMiddle(taskId, 20), inMiddle(cluster, 20),
                inMiddle(streamName + " " + taskType, 90));
        System.out.println(generateEdge(130));
        HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = helixDataAccessor.keyBuilder();
        IdealState assignment = helixDataAccessor.getProperty(keyBuilder.idealStates(taskId));
        ExternalView view = helixDataAccessor.getProperty(keyBuilder.externalView(taskId));
        List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder.liveInstances());
        System.out.format("%-50s%-100s%n", inMiddle("Partition", 50), inMiddle("State", 20));
        System.out.println(generateEdge(130));
        for (String partition : assignment.getPartitionSet()) {
            Map<String, String> stateMap = view.getStateMap(partition);
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
            System.out.format("%-50s%-10s%n", inMiddle(partition, 50), inMiddle(sb.toString(), 100));
        }
        System.out.println(generateEdge(130));
    }

    private static void printAppInfo(HelixManager manager, String cluster, String app) {
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(cluster).forResource(app).build();
        String uri = configAccessor.get(scope, "s4r_uri");

        System.out.println("App Status");
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", inMiddle("Name", 20), inMiddle("Cluster", 20), inMiddle("URI", 90));
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", inMiddle(app, 20), inMiddle(cluster, 20), inMiddle(uri, 90));
        System.out.println(generateEdge(130));
        HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = helixDataAccessor.keyBuilder();
        IdealState assignment = helixDataAccessor.getProperty(keyBuilder.idealStates(app));
        ExternalView view = helixDataAccessor.getProperty(keyBuilder.externalView(app));
        List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder.liveInstances());
        Map<String, String> assignmentMap = assignment.getInstanceStateMap(app);
        Map<String, String> appStateMap = view.getStateMap(app);
        System.out.format("%-50s%-20s%n", inMiddle("Node id", 50), inMiddle("DEPLOYED", 20));
        System.out.println(generateEdge(130));
        for (String instance : assignmentMap.keySet()) {
            String state = appStateMap.get(instance);
            System.out.format("%-50s%-10s%n", inMiddle(instance, 50),
                    inMiddle((("ONLINE".equals(state) && liveInstances.contains(instance)) ? "Y" : "N"), 20));
        }

        System.out.println(generateEdge(130));

    }

    private static void printClusterInfo(HelixManager manager, String cluster) {
        HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
        Builder keyBuilder = dataAccessor.keyBuilder();
        List<String> instances = dataAccessor.getChildNames(keyBuilder.instanceConfigs());
        List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
        if (liveInstances == null) {
            liveInstances = Collections.emptyList();
        }
        System.out.println("Cluster Status");
        System.out.println(generateEdge(130));
        System.out.format("%-50s%-80s%n", " ", inMiddle("Nodes", 80));
        System.out.format("%-20s%-20s%-10s%s%n", inMiddle("Cluster Name", 20), inMiddle("Nodes", 20),
                inMiddle("Active", 10), generateEdge(80));
        System.out.format("%-54s%-10s%-50s%-8s%-8s%n", " ", inMiddle("Node id", 10), inMiddle("Host", 50),
                inMiddle("Port", 8), inMiddle("Active", 10));
        System.out.println(generateEdge(130));

        System.out.format("%-20s%-20s%-10s", inMiddle(cluster, 20), inMiddle("" + instances.size(), 8),
                inMiddle("" + liveInstances.size(), 8));
        boolean first = true;

        for (String instance : instances) {
            InstanceConfig config = dataAccessor.getProperty(keyBuilder.instanceConfig(instance));
            // System.out.println(config);
            if (first) {
                first = false;
            } else {
                System.out.format("%n%-50s", " ");
            }
            System.out.format("%-10s%-46s%-10s%-10s", inMiddle("" + config.getId(), 10),
                    inMiddle(config.getHostName(), 50), inMiddle(config.getPort() + "", 10),
                    inMiddle(liveInstances.contains(config.getInstanceName()) ? "Y" : "N", 10));
        }

        System.out.println();
    }

    @Parameters(commandNames = "s4 status", commandDescription = "Show status of S4", separators = "=")
    static class StatusArgs extends S4ArgsBase {

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

    private static void showAppsStatus(List<Cluster> clusters) {
        System.out.println("App Status");
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", inMiddle("Name", 20), inMiddle("Cluster", 20), inMiddle("URI", 90));
        System.out.println(generateEdge(130));
        for (Cluster cluster : clusters) {
            if (!NONE.equals(cluster.app.name)) {
                System.out.format("%-20s%-20s%-90s%n", inMiddle(cluster.app.name, 20),
                        inMiddle(cluster.app.cluster, 20), cluster.app.uri);
            }
        }
        System.out.println(generateEdge(130));

    }

    private static void showClustersStatus(List<Cluster> clusters) {
        System.out.println("Cluster Status");
        System.out.println(generateEdge(130));
        System.out.format("%-50s%-80s%n", " ", inMiddle("Active nodes", 80));
        System.out.format("%-20s%-20s%-10s%s%n", inMiddle("Name", 20), inMiddle("App", 20), inMiddle("Tasks", 10),
                generateEdge(80));
        System.out.format("%-50s%-10s%-10s%-50s%-10s%n", " ", inMiddle("Number", 8), inMiddle("Task id", 10),
                inMiddle("Host", 50), inMiddle("Port", 8));
        System.out.println(generateEdge(130));

        for (Cluster cluster : clusters) {
            System.out.format("%-20s%-20s%-10s%-10s", inMiddle(cluster.clusterName, 20),
                    inMiddle(cluster.app.name, 20), inMiddle("" + cluster.taskNumber, 8),
                    inMiddle("" + cluster.nodes.size(), 8));
            boolean first = true;
            for (ClusterNode node : cluster.nodes) {
                if (first) {
                    first = false;
                } else {
                    System.out.format("%n%-60s", " ");
                }
                System.out.format("%-10s%-50s%-10s", inMiddle("" + node.getTaskId(), 10),
                        inMiddle(node.getMachineName(), 50), inMiddle(node.getPort() + "", 10));
            }
            System.out.println();
        }
        System.out.println(generateEdge(130));
    }

    private static void showStreamsStatus(List<Stream> streams) {
        System.out.println("Stream Status");
        System.out.println(generateEdge(130));
        System.out.format("%-20s%-55s%-55s%n", inMiddle("Name", 20), inMiddle("Producers", 55),
                inMiddle("Consumers", 55));
        System.out.println(generateEdge(130));

        for (Stream stream : streams) {
            System.out.format("%-20s%-55s%-55s%n", inMiddle(stream.streamName, 20),
                    inMiddle(getFormatString(stream.producers, stream.clusterAppMap), 55),
                    inMiddle(getFormatString(stream.consumers, stream.clusterAppMap), 55));
        }
        System.out.println(generateEdge(130));

    }

    private static String inMiddle(String content, int width) {
        int i = (width - content.length()) / 2;
        return String.format("%" + i + "s%s", " ", content);
    }

    private static String generateEdge(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("-");
        }
        return sb.toString();
    }

    /**
     * show as cluster1(app1), cluster2(app2)
     * 
     * @param clusters
     *            cluster list
     * @param clusterAppMap
     *            <cluster,app>
     * @return
     */
    private static String getFormatString(Collection<String> clusters, Map<String, String> clusterAppMap) {
        if (clusters == null || clusters.size() == 0) {
            return NONE;
        } else {
            // show as: cluster1(app1), cluster2(app2)
            StringBuilder sb = new StringBuilder();
            for (String cluster : clusters) {
                String app = clusterAppMap.get(cluster);
                sb.append(cluster);
                if (!NONE.equals(app)) {
                    sb.append("(").append(app).append(")");
                }
                sb.append(" ");
            }
            return sb.toString();
        }
    }

    static class Stream {

        private final ZkClient zkClient;
        private final String consumerPath;
        private final String producerPath;

        String streamName;
        Set<String> producers = new HashSet<String>();// cluster name
        Set<String> consumers = new HashSet<String>();// cluster name

        Map<String, String> clusterAppMap = Maps.newHashMap();

        public Stream(String streamName, ZkClient zkClient) throws Exception {
            this.streamName = streamName;
            this.zkClient = zkClient;
            this.consumerPath = "/s4/streams/" + streamName + "/consumers";
            this.producerPath = "/s4/streams/" + streamName + "/producers";
            readStreamFromZk();
        }

        private void readStreamFromZk() throws Exception {
            List<String> consumerNodes = zkClient.getChildren(consumerPath);
            for (String node : consumerNodes) {
                ZNRecord consumer = zkClient.readData(consumerPath + "/" + node, true);
                consumers.add(consumer.getSimpleField("clusterName"));
            }

            List<String> producerNodes = zkClient.getChildren(producerPath);
            for (String node : producerNodes) {
                ZNRecord consumer = zkClient.readData(producerPath + "/" + node, true);
                producers.add(consumer.getSimpleField("clusterName"));
            }

            getAppNames();
        }

        private void getAppNames() {
            Set<String> clusters = new HashSet<String>(consumers);
            clusters.addAll(producers);
            for (String cluster : clusters) {
                clusterAppMap.put(cluster, getApp(cluster, zkClient));
            }
        }

        public boolean containsCluster(String cluster) {
            if (producers.contains(cluster) || consumers.contains(cluster)) {
                return true;
            }
            return false;
        }

        private static String getApp(String clusterName, ZkClient zkClient) {
            String appPath = "/s4/clusters/" + clusterName + "/app/s4App";
            if (zkClient.exists(appPath)) {
                ZNRecord appRecord = zkClient.readData("/s4/clusters/" + clusterName + "/app/s4App");
                return appRecord.getSimpleField("name");
            }
            return NONE;
        }
    }

    static class App {
        private String name = NONE;
        private String cluster;
        private String uri = NONE;
    }

    static class Cluster {
        private final ZkClient zkClient;
        private final String taskPath;
        private final String processPath;
        private final String appPath;

        String clusterName;
        int taskNumber;
        App app;

        List<ClusterNode> nodes = new ArrayList<ClusterNode>();

        public Cluster(String clusterName, ZkClient zkClient) throws Exception {
            this.clusterName = clusterName;
            this.zkClient = zkClient;
            this.taskPath = "/s4/clusters/" + clusterName + "/tasks";
            this.processPath = "/s4/clusters/" + clusterName + "/process";
            this.appPath = "/s4/clusters/" + clusterName + "/app/s4App";
            readClusterFromZk();
        }

        public void readClusterFromZk() throws Exception {
            List<String> processes;
            List<String> tasks;

            tasks = zkClient.getChildren(taskPath);
            processes = zkClient.getChildren(processPath);

            taskNumber = tasks.size();

            for (int i = 0; i < processes.size(); i++) {
                ZNRecord process = zkClient.readData(processPath + "/" + processes.get(i), true);
                if (process != null) {
                    int partition = Integer.parseInt(process.getSimpleField("partition"));
                    String host = process.getSimpleField("host");
                    int port = Integer.parseInt(process.getSimpleField("port"));
                    String taskId = process.getSimpleField("taskId");
                    ClusterNode node = new ClusterNode(partition, port, host, taskId);
                    nodes.add(node);
                }
            }

            app = new App();
            app.cluster = clusterName;
            try {
                ZNRecord appRecord = zkClient.readData(appPath);
                app.name = appRecord.getSimpleField("name");
                app.uri = appRecord.getSimpleField("s4r_uri");
            } catch (ZkNoNodeException e) {
                logger.warn(appPath + " doesn't exist");
            }
        }

    }

}
