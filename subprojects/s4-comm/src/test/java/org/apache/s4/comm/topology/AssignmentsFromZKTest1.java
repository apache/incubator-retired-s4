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
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

/**
 * Separated from AssignmentsFromZKTest2 so that VM exit upon Zookeeper connection expiration does not affect the test
 * in that other class.
 * 
 */
public class AssignmentsFromZKTest1 extends ZkBasedTest {

    @Test
    public void testAssignmentFor1Cluster() throws Exception {
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String topologyNames = "cluster1";
        testAssignment(taskSetup, topologyNames);
    }

    public static void testAssignment(TaskSetup taskSetup, final String topologyNames) throws InterruptedException {
        final Set<String> names = Sets.newHashSet(Splitter.onPattern("\\s*,\\s*").split(topologyNames));
        taskSetup.clean("s4");
        for (String topologyName : names) {
            taskSetup.setup(topologyName, 10, 1300);
        }

        final CountDownLatch latch = new CountDownLatch(10 * names.size());
        for (int i = 0; i < 10; i++) {
            Runnable runnable = new Runnable() {

                @SuppressWarnings("unused")
                @Override
                public void run() {
                    AssignmentFromZK assignmentFromZK;
                    try {

                        for (String topologyName : names) {
                            ZkClient zkClient = new ZkClient(CommTestUtils.ZK_STRING);
                            zkClient.setZkSerializer(new ZNRecordSerializer());
                            assignmentFromZK = new AssignmentFromZK(topologyName, 30000, zkClient);
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

        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertEquals(true, await);
    }
}
