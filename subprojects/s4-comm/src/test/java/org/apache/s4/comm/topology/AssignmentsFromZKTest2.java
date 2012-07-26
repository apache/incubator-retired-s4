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

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

/**
 * Separated from AssignmentsFromZKTest1 so that VM exit upon Zookeeper connection expiration does not affect the test
 * in that other class.
 * 
 */
public class AssignmentsFromZKTest2 extends ZkBasedTest {

    @Test
    public void testAssignmentFor2Clusters() throws Exception {
        Thread.sleep(2000);
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String topologyNames = "cluster2, cluster3";
        AssignmentsFromZKTest1.testAssignment(taskSetup, topologyNames);
    }

}
