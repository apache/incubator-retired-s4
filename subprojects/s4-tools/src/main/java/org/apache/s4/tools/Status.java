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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Maps;

public class Status extends S4ArgsBase {
    static Logger logger = LoggerFactory.getLogger(Status.class);

    public static void main(String[] args) {

        StatusArgs statusArgs = new StatusArgs();
        Tools.parseArgs(statusArgs, args);

        List<Cluster> clusterStatus = new ArrayList<Cluster>();
        List<Stream> streamStatus = new ArrayList<Stream>();

        try {
            ZkClient zkClient = new ZkClient(statusArgs.zkConnectionString, statusArgs.timeout);
            zkClient.setZkSerializer(new ZNRecordSerializer());

            List<String> clusters = statusArgs.clusters;
            if (clusters == null) {
                // Load all subclusters
                clusters = zkClient.getChildren("/s4/clusters");
            }

            Set<String> app = null;
            Set<String> requiredAppCluster = new HashSet<String>();
            if (statusArgs.apps != null) {
                app = new HashSet<String>(statusArgs.apps);
            }

            for (String clusterName : clusters) {
                try {
                    if (zkClient.exists("/s4/clusters/" + clusterName)) {
                        Cluster cluster = new Cluster(clusterName, zkClient);
                        if (app == null || app.contains(cluster.app.name)) {
                            clusterStatus.add(cluster);
                            requiredAppCluster.add(cluster.clusterName);
                        }
                    } else {
                        logger.error("/s4/clusters/" + clusterName + " doesn't exist");
                    }
                } catch (Exception e) {
                    logger.error("Cannot get the status of " + clusterName, e);
                }
            }

            List<String> streams = statusArgs.streams;
            if (streams == null) {
                // Load all streams published
                streams = zkClient.getChildren("/s4/streams");
            }

            for (String streamName : streams) {
                try {
                    if (zkClient.exists("/s4/streams/" + streamName)) {
                        Stream stream = new Stream(streamName, zkClient);
                        if (app == null) {
                            streamStatus.add(stream);
                        } else {
                            for (String cluster : requiredAppCluster) {
                                if (stream.containsCluster(cluster)) {
                                    streamStatus.add(stream);
                                    break;
                                }
                            }
                        }
                    } else {
                        logger.error("/s4/streams/" + streamName + " doesn't exist");
                    }
                } catch (Exception e) {
                    logger.error("Cannot get the status of " + streamName, e);
                }
            }

            System.out.println();
            showAppsStatus(clusterStatus);
            System.out.println("\n\n");
            showClustersStatus(clusterStatus);
            System.out.println("\n\n");
            showStreamsStatus(streamStatus);
            System.out.println("\n\n");

        } catch (Exception e) {
            logger.error("Cannot get the status of S4", e);
        }

    }

    @Parameters(commandNames = "s4 status", commandDescription = "Show status of S4", separators = "=")
    static class StatusArgs extends S4ArgsBase {

        @Parameter(names = { "-app" }, description = "Only show status of specified S4 application(s)", required = false)
        List<String> apps;

        @Parameter(names = { "-c", "-cluster" }, description = "Only show status of specified S4 cluster(s)", required = false)
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
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-20s%-20s%-90s%n", StatusUtils.inMiddle("Name", 20), StatusUtils.inMiddle("Cluster", 20), StatusUtils.inMiddle("URI", 90));
        System.out.println(StatusUtils.generateEdge(130));
        for (Cluster cluster : clusters) {
            if (!StatusUtils.NONE.equals(cluster.app.name)) {
                System.out.format("%-20s%-20s%-90s%n", StatusUtils.inMiddle(cluster.app.name, 20),
                        StatusUtils.inMiddle(cluster.app.cluster, 20), cluster.app.uri);
            }
        }
        System.out.println(StatusUtils.generateEdge(130));

    }

    private static void showClustersStatus(List<Cluster> clusters) {
        System.out.println("Cluster Status");
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-50s%-80s%n", " ", StatusUtils.inMiddle("Active nodes", 80));
        System.out.format("%-20s%-20s%-10s%s%n", StatusUtils.inMiddle("Name", 20), StatusUtils.inMiddle("App", 20), StatusUtils.inMiddle("Tasks", 10),
                StatusUtils.generateEdge(80));
        System.out.format("%-50s%-10s%-10s%-50s%-10s%n", " ", StatusUtils.inMiddle("Number", 8), StatusUtils.inMiddle("Task id", 10),
                StatusUtils.inMiddle("Host", 50), StatusUtils.inMiddle("Port", 8));
        System.out.println(StatusUtils.generateEdge(130));

        for (Cluster cluster : clusters) {
            System.out.format("%-20s%-20s%-10s%-10s", StatusUtils.inMiddle(cluster.clusterName, 20),
                    StatusUtils.inMiddle(cluster.app.name, 20), StatusUtils.inMiddle("" + cluster.taskNumber, 8),
                    StatusUtils.inMiddle("" + cluster.nodes.size(), 8));
            boolean first = true;
            for (ClusterNode node : cluster.nodes) {
                if (first) {
                    first = false;
                } else {
                    System.out.format("%n%-60s", " ");
                }
                System.out.format("%-10s%-50s%-10s", StatusUtils.inMiddle("" + node.getTaskId(), 10),
                        StatusUtils.inMiddle(node.getMachineName(), 50), StatusUtils.inMiddle(node.getPort() + "", 10));
            }
            System.out.println();
        }
        System.out.println(StatusUtils.generateEdge(130));
    }

    private static void showStreamsStatus(List<Stream> streams) {
        System.out.println("Stream Status");
        System.out.println(StatusUtils.generateEdge(130));
        System.out.format("%-20s%-55s%-55s%n", StatusUtils.inMiddle("Name", 20), StatusUtils.inMiddle("Producers", 55),
                StatusUtils.inMiddle("Consumers", 55));
        System.out.println(StatusUtils.generateEdge(130));

        for (Stream stream : streams) {
            System.out.format("%-20s%-55s%-55s%n", StatusUtils.inMiddle(stream.streamName, 20),
                    StatusUtils.inMiddle(StatusUtils.getFormatString(stream.producers, stream.clusterAppMap), 55),
                    StatusUtils.inMiddle(StatusUtils.getFormatString(stream.consumers, stream.clusterAppMap), 55));
        }
        System.out.println(StatusUtils.generateEdge(130));

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
                AppConfig appConfig = new AppConfig((ZNRecord) zkClient.readData("/s4/clusters/" + clusterName
                        + "/app/s4App"));
                return appConfig.getAppName();
            }
            return StatusUtils.NONE;
        }
    }

    static class App {
        private String name = StatusUtils.NONE;
        private String cluster;
        private String uri = StatusUtils.NONE;
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
                AppConfig appConfig = new AppConfig(appRecord);
                app.name = appConfig.getAppName();
                app.uri = appConfig.getAppURI();
            } catch (ZkNoNodeException e) {
                logger.warn(appPath + " doesn't exist");
            }
        }

    }

}
