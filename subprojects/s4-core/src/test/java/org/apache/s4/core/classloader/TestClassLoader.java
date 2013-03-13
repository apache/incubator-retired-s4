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

package org.apache.s4.core.classloader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZNRecord;
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

public class TestClassLoader {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedProducerNode;
    private ZkClient zkClient;
    private final static String PRODUCER_CLUSTER = "producerCluster";
    private static File producerS4rDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        producerS4rDir = new File(gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build/libs");

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/producer-app/build.gradle"), "clean", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build.gradle"});
        Assert.assertFalse(producerS4rDir.exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/producer-app/build.gradle"), "s4r", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/producer-app/build.gradle",
                "appClass=s4app.ProducerApp", "appName=testApp"});

        Assert.assertTrue(new File(producerS4rDir, "testApp-0.0.0-SNAPSHOT.s4r").exists());
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
    }

    @Test
    public void testInitialDeploymentFromFileSystem() throws Exception {

        File producerS4R = new File(producerS4rDir, "testApp-0.0.0-SNAPSHOT.s4r");
        String uriProducer = producerS4R.toURI().toString();

        initializeS4Node();

        final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1);
        zkClient.subscribeDataChanges("/s4/classLoader", new IZkDataListener() {

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                queue.put(data);
            }
        });
        DeploymentUtils.initAppConfig(new AppConfig.Builder().appURI(uriProducer).build(), PRODUCER_CLUSTER, true,
                "localhost:2181");

        Object classLoaderRecord = queue.poll(20, TimeUnit.SECONDS);
        assertTrue("Stored record has unexpected type", classLoaderRecord instanceof ZNRecord);
        ZNRecord record = (ZNRecord) classLoaderRecord;

        assertEquals("Unexpected classloader runs the app init()", S4RLoader.class.getName(), record.getId());
    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException, KeeperException {
        // 1. start s4 node. Check that no app is deployed.
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean("s4");
        taskSetup.setup(PRODUCER_CLUSTER, 1, 1300);

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

        forkedProducerNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + PRODUCER_CLUSTER }, zkClient, 20,
                PRODUCER_CLUSTER);
        Assert.assertTrue(signalProcessesReady.await(20, TimeUnit.SECONDS));

    }
}
