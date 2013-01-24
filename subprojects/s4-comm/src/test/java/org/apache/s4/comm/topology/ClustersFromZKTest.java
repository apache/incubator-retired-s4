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

package org.apache.s4.comm.topology;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class ClustersFromZKTest extends ZkBasedTest {

    @Test
    @Ignore
    public void testAssignmentFor1Topology() throws InterruptedException, Exception {
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String clustersString = "cluster1";
        testAssignment(taskSetup, clustersString);
    }

    @Test
    public void testAssignmentFor2Topologies() throws Exception {
        Thread.sleep(2000);
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String clustersString = "cluster2, cluster3";
        testAssignment(taskSetup, clustersString);

    }

    private void testAssignment(TaskSetup taskSetup, final String clustersString) throws Exception,
            InterruptedException {
        final Set<String> clusterNames = Sets.newHashSet(Splitter.onPattern("\\s*,\\s*").split(clustersString));
        taskSetup.clean("s4");

        for (String clusterName : clusterNames) {
            taskSetup.setup(clusterName, 10, 1300);
        }
        ZkClient zkClient1 = new ZkClient(CommTestUtils.ZK_STRING);
        zkClient1.setZkSerializer(new ZNRecordSerializer());
        final ClustersFromZK clusterFromZK = new ClustersFromZK(null, 30000, zkClient1);

        final CountDownLatch signalAllClustersComplete = new CountDownLatch(clusterNames.size());
        for (final String clusterName : clusterNames) {
            ClusterChangeListener listener = new ClusterChangeListener() {

                @Override
                public void onChange() {
                    if (clusterFromZK.getCluster(clusterName).getPhysicalCluster().getNodes().size() == 10) {
                        signalAllClustersComplete.countDown();
                    }

                }
            };
            clusterFromZK.getCluster(clusterName).addListener(listener);
        }

        final CountDownLatch latch = new CountDownLatch(10 * clusterNames.size());
        for (int i = 0; i < 10; i++) {
            Runnable runnable = new Runnable() {

                @SuppressWarnings("unused")
                @Override
                public void run() {
                    AssignmentFromZK assignmentFromZK;
                    try {
                        for (String clusterName : clusterNames) {
                            ZkClient zkClient = new ZkClient(CommTestUtils.ZK_STRING);
                            zkClient.setZkSerializer(new ZNRecordSerializer());
                            assignmentFromZK = new AssignmentFromZK(clusterName, 30000, zkClient);
                            assignmentFromZK.init();
                            ClusterNode assignClusterNode = assignmentFromZK.assignClusterNode();
                            latch.countDown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            Thread t = new Thread(runnable);
            t.start();
        }

        boolean await = latch.await(20, TimeUnit.SECONDS);
        assertEquals(true, await);
        boolean success = false;
        success = signalAllClustersComplete.await(20, TimeUnit.SECONDS);
        assertEquals(true, success);
        for (String clusterName : clusterNames) {
            if (!(10 == clusterFromZK.getCluster(clusterName).getPhysicalCluster().getNodes().size())) {
                // pending zookeeper updates are not yet reflected
                Thread.sleep(2000);
            }
            assertEquals(10, clusterFromZK.getCluster(clusterName).getPhysicalCluster().getNodes().size());
        }
    }
}
