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
