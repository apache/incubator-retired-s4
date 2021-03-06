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

package org.apache.s4.deploy.prodcon;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DistributedDeploymentManager;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.sun.net.httpserver.HttpServer;

public class TestProducerConsumer {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedProducerNode;
    private Process forkedConsumerNode;
    private ZkClient zkClient;
    private final static String PRODUCER_CLUSTER = "producerCluster";
    private final static String CONSUMER_CLUSTER = "consumerCluster";
    private HttpServer httpServer;
    private static File tmpAppsDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        tmpAppsDir = Files.createTempDir();

        Assert.assertTrue(tmpAppsDir.exists());
        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/producer-app/build.gradle"), "installS4R",
                new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/consumer-app/build.gradle"), "installS4R",
                new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });
    }

    @Before
    public void prepare() throws Exception {
        CommTestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = CommTestUtils.startZookeeperServer();
        final ZooKeeper zk = CommTestUtils.createZkClient();
        try {
            zk.delete("/simpleAppCreated", -1);
        } catch (Exception ignored) {
        }

        zk.close();
    }

    @After
    public void cleanup() throws Exception {
        CommTestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        CommTestUtils.killS4App(forkedProducerNode);
        CommTestUtils.killS4App(forkedConsumerNode);
    }

    private PropertiesConfiguration loadConfig() throws org.apache.commons.configuration.ConfigurationException,
            IOException {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(Resources.getResource("default.s4.core.properties").openStream());
        return config;
    }

    @Test
    public void testInitialDeploymentFromFileSystem() throws Exception {

        File producerS4R = new File(tmpAppsDir, "producer-app-0.0.0-SNAPSHOT.s4r");
        String uriProducer = producerS4R.toURI().toString();

        File consumerS4R = new File(tmpAppsDir, "consumer-app-0.0.0-SNAPSHOT.s4r");
        String uriConsumer = consumerS4R.toURI().toString();

        initializeS4Node();

        CountDownLatch signalConsumptionComplete = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/AllTicksReceived", signalConsumptionComplete,
                CommTestUtils.createZkClient());

        boolean consumerStreamReady = true;
        try {
            zkClient.getChildren("/s4/streams/tickStream/consumers");
        } catch (ZkNoNodeException e) {
            consumerStreamReady = false;
        }
        Assert.assertFalse(consumerStreamReady);
        final CountDownLatch signalConsumerReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/streams/tickStream/consumers", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 1) {
                    signalConsumerReady.countDown();
                }

            }
        });

        ZNRecord record2 = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        record2.putSimpleField(DistributedDeploymentManager.S4R_URI, uriConsumer);
        zkClient.create("/s4/clusters/" + CONSUMER_CLUSTER + "/app/s4App", record2, CreateMode.PERSISTENT);
        // TODO check that consumer app is ready with a better way than checking stream consumers
        Assert.assertTrue(signalConsumerReady.await(20, TimeUnit.SECONDS));

        ZNRecord record1 = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        record1.putSimpleField(DistributedDeploymentManager.S4R_URI, uriProducer);
        zkClient.create("/s4/clusters/" + PRODUCER_CLUSTER + "/app/s4App", record1, CreateMode.PERSISTENT);

        // that may be a bit long to complete...
        Assert.assertTrue(signalConsumptionComplete.await(30, TimeUnit.SECONDS));

    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException, KeeperException {
        // 1. start s4 nodes. Check that no app is deployed.
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean("s4");
        taskSetup.setup(PRODUCER_CLUSTER, 1, 1300);

        TaskSetup taskSetup2 = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup2.setup(CONSUMER_CLUSTER, 1, 1400);

        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        List<String> processes = zkClient.getChildren("/s4/clusters/" + PRODUCER_CLUSTER + "/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/" + PRODUCER_CLUSTER + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 1) {
                    signalProcessesReady.countDown();
                }

            }
        });

        zkClient.subscribeChildChanges("/s4/clusters/" + CONSUMER_CLUSTER + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 1) {
                    signalProcessesReady.countDown();
                }

            }
        });

        forkedProducerNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + PRODUCER_CLUSTER });
        forkedConsumerNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + CONSUMER_CLUSTER });

        // TODO synchro with ready state from zk
        // Thread.sleep(10000);
        Assert.assertTrue(signalProcessesReady.await(20, TimeUnit.SECONDS));

    }
}
