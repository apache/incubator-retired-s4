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

package org.apache.s4.deploy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.s4.base.Event;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.S4RHttpServer;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Injector;

/**
 * Tests deployment of packaged applications <br>
 * - loaded from local apps directory <br>
 * - deployed through zookeeper notification <br>
 * - ... from the file system <br>
 * - ... or from a web server
 * 
 */
public class TestAutomaticDeployment extends ZkBasedTest {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedNode;
    private ZkClient zkClient;
    private S4RHttpServer s4rHttpServer;
    public static File s4rDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {

        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        s4rDir = new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build/libs");
        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build.gradle"), "clean", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/simple-deployable-app-1/build.gradle" });
        Assert.assertFalse(s4rDir.exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build.gradle"), "s4r", new String[] { "-buildFile="
                + gradlewFile.getParentFile().getAbsolutePath() + "/test-apps/simple-deployable-app-1/build.gradle", 
                "appClass=org.apache.s4.deploy.TestApp", "appName=simpleApp" });
        Assert.assertTrue(new File(s4rDir, "simpleApp-0.0.0-SNAPSHOT.s4r").exists());
    }

    @Test
    public void testZkTriggeredDeploymentFromFileSystem() throws Exception {

        initializeS4Node();

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        final String uri = new File(s4rDir, "simpleApp-0.0.0-SNAPSHOT.s4r").toURI().toString();

        assertDeployment(uri, zkClient, true);

    }

    public static void assertDeployment(final String uri, ZkClient zkClient, boolean createZkAppNode)
            throws KeeperException, InterruptedException, IOException {
        CountDownLatch signalAppInitialized = new CountDownLatch(1);
        CountDownLatch signalAppStarted = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppInitialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppStarted,
                CommTestUtils.createZkClient());

        if (createZkAppNode) {
            // otherwise we need to do that through a separate tool
            DeploymentUtils.initAppConfig(new AppConfig.Builder().appURI(uri).build(), "cluster1", true,
                    "localhost:2181");
        }

        Assert.assertTrue(signalAppInitialized.await(20, TimeUnit.SECONDS));
        Assert.assertTrue(signalAppStarted.await(20, TimeUnit.SECONDS));

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        CommTestUtils
                .watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, CommTestUtils.createZkClient());

        Injector injector = CoreTestUtils.createInjectorWithNonFailFastZKClients();

        TCPEmitter emitter = injector.getInstance(TCPEmitter.class);

        Event event = new Event();
        event.setStreamId("inputStream");
        event.put("line", String.class, time1);
        emitter.send(
                0,
                injector.getInstance(SerializerDeserializerFactory.class)
                        .createSerializerDeserializer(Thread.currentThread().getContextClassLoader()).serialize(event));

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testZkTriggeredDeploymentFromHttp() throws Exception {
        initializeS4Node();

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        File tmpDir = Files.createTempDir();

        File s4rToDeploy = new File(tmpDir, String.valueOf(System.currentTimeMillis()));

        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(s4rDir, "/simpleApp-0.0.0-SNAPSHOT.s4r")),
                Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        // we start a
        s4rHttpServer = new S4RHttpServer(8080, tmpDir);
        s4rHttpServer.start();

        assertDeployment("http://localhost:8080/s4/" + s4rToDeploy.getName(), zkClient, true);

        // check resource loading (we use a zkclient without custom serializer)
        ZkClient client2 = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        Assert.assertEquals("Salut!", client2.readData("/resourceData"));
        client2.close();

    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException {
        // 0. package s4 app
        // TODO this is currently done offline, and the app contains the TestApp class copied from the one in the
        // current package .

        // 1. start s4 nodes. Check that no app is deployed.
        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());

        checkNoAppAlreadyDeployed(zkClient);

        forkedNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=cluster1" }, zkClient, 10, "cluster1");

    }

    public static void checkNoAppAlreadyDeployed(ZkClient zkClient) {
        List<String> processes = zkClient.getChildren("/s4/clusters/cluster1/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/cluster1/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 2) {
                    signalProcessesReady.countDown();
                }

            }
        });

    }

    @After
    public void cleanup() throws Exception {
        CommTestUtils.killS4App(forkedNode);
        if (s4rHttpServer != null) {
            s4rHttpServer.stop();
        }
    }

    public static void main(String[] args) throws IOException {

        System.out.println("Server is listening on port 8080");
    }
}
