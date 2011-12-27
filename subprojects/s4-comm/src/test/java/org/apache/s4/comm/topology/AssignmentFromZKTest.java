package org.apache.s4.comm.topology;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

import org.apache.s4.comm.tools.TaskSetup;
import org.junit.Test;

public class AssignmentFromZKTest extends ZKBaseTest {

    @Test
    public void testAssignment() throws Exception {
        TaskSetup taskSetup = new TaskSetup(zookeeperAddress);
        final String clusterName = "test-s4-cluster";
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 10);
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            Runnable runnable = new Runnable() {

                @SuppressWarnings("unused")
                @Override
                public void run() {
                    AssignmentFromZK assignmentFromZK;
                    try {
                        assignmentFromZK = new AssignmentFromZK(clusterName, zookeeperAddress, 30000, 30000);
                        ClusterNode assignClusterNode = assignmentFromZK.assignClusterNode();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            Thread t = new Thread(runnable);
            t.start();
        }

        boolean await = latch.await(3, TimeUnit.SECONDS);
        assertEquals(true, await);
    }
}
