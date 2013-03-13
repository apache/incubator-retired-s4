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
import java.io.IOException;

import junit.framework.Assert;

import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.deploy.AppConstants;
import org.apache.s4.deploy.TestAutomaticDeployment;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.S4RHttpServer;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.After;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class TestDeploy extends ZkBasedTest {

    private Process forkedNode;
    private S4RHttpServer s4rHttpServer;

    @Test
    public void testDeployment() throws Exception {
        TestAutomaticDeployment.createS4RFiles();

        ZkClient zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());

        TestAutomaticDeployment.checkNoAppAlreadyDeployed(zkClient);

        forkedNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=cluster1" }, zkClient, 10, "cluster1");

        // deploy app

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        File tmpDir = Files.createTempDir();

        File s4rToDeploy = new File(tmpDir, String.valueOf(System.currentTimeMillis()));

        Assert.assertTrue(ByteStreams.copy(Files.newInputStreamSupplier(new File(TestAutomaticDeployment.s4rDir,
                "simpleApp-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        s4rHttpServer = new S4RHttpServer(8080, tmpDir);
        s4rHttpServer.start();

        Deploy.main(new String[] { "-s4r", "http://localhost:8080/s4/" + s4rToDeploy.getName(), "-c", "cluster1",
                "-appName", "toto", "-testMode" });

        TestAutomaticDeployment.assertDeployment("http://localhost:8080/s4/" + s4rToDeploy.getName(), zkClient, false);

        // check resource loading (we use a zkclient without custom serializer)
        ZkClient client2 = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        Assert.assertEquals("Salut!", client2.readData("/resourceData"));

    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        CoreTestUtils.killS4App(forkedNode);
        if (s4rHttpServer != null) {
            s4rHttpServer.stop();
        }
    }
}
