package org.apache.s4.comm.topology;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.fixtures.CommTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class AssignmentsFromZKTest extends ZKBaseTest {

    @Test
    @Ignore
    public void testAssignmentFor1Cluster() throws Exception {
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String topologyNames = "cluster1";
        testAssignment(taskSetup, topologyNames);
    }

    @Test
    public void testAssignmentFor2Clusters() throws Exception {
        Thread.sleep(2000);
        TaskSetup taskSetup = new TaskSetup(CommTestUtils.ZK_STRING);
        final String topologyNames = "cluster2, cluster3";
        testAssignment(taskSetup, topologyNames);
    }

    private void testAssignment(TaskSetup taskSetup, final String topologyNames) throws InterruptedException {
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
                            assignmentFromZK = new AssignmentFromZK(topologyName, CommTestUtils.ZK_STRING, 30000, 30000);
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
