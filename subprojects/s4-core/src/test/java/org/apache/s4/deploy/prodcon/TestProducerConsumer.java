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
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    private static File producerS4rDir;
    private static File consumerS4rDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        producerS4rDir = new File(gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build/libs");
        consumerS4rDir = new File(gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/consumer-app/build/libs");

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/producer-app/build.gradle"), "clean", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build.gradle" });
        Assert.assertFalse(producerS4rDir.exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/producer-app/build.gradle"), "s4r", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build.gradle",
                "appClass=s4app.ProducerApp", "appName=producer" });

        Assert.assertTrue(new File(producerS4rDir, "producer-0.0.0-SNAPSHOT.s4r").exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/consumer-app/build.gradle"), "clean", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/consumer-app/build.gradle" });
        Assert.assertFalse(consumerS4rDir.exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/consumer-app/build.gradle"), "s4r", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/consumer-app/build.gradle",
                "appClass=s4app.ConsumerApp", "appName=consumer" });

        Assert.assertTrue(new File(consumerS4rDir, "consumer-0.0.0-SNAPSHOT.s4r").exists());
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

        File producerS4R = new File(producerS4rDir, "producer-0.0.0-SNAPSHOT.s4r");
        String uriProducer = producerS4R.toURI().toString();

        File consumerS4R = new File(consumerS4rDir, "consumer-0.0.0-SNAPSHOT.s4r");
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

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appURI(uriConsumer).build(), CONSUMER_CLUSTER, true,
                "localhost:2181");
        // TODO check that consumer app is ready with a better way than checking stream consumers
        Assert.assertTrue(signalConsumerReady.await(20, TimeUnit.SECONDS));

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appURI(uriProducer).build(), PRODUCER_CLUSTER, true,
                "localhost:2181");

        // that may be a bit long to complete...
        Assert.assertTrue(signalConsumptionComplete.await(40, TimeUnit.SECONDS));

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

        forkedProducerNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + PRODUCER_CLUSTER }, zkClient, 20,
                PRODUCER_CLUSTER);
        forkedConsumerNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + CONSUMER_CLUSTER }, zkClient, 20,
                CONSUMER_CLUSTER);

        Assert.assertTrue(signalProcessesReady.await(20, TimeUnit.SECONDS));

    }
}
