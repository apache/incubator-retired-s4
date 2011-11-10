package org.apache.s4.comm.topology;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static org.junit.Assert.*;

import org.apache.s4.comm.tools.TaskSetup;
import org.junit.Test;

public class TopologyFromZKTest extends ZKBaseTest {

    @Test
    public void testAssignment() throws Exception {
        TaskSetup taskSetup = new TaskSetup(zookeeperAddress);
        final String clusterName = "test-s4-cluster";
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 10);

        final TopologyFromZK topologyFromZK = new TopologyFromZK(clusterName, zookeeperAddress, 30000, 30000);
        final Lock lock = new ReentrantLock();
        final Condition signal = lock.newCondition();
        TopologyChangeListener listener = new TopologyChangeListener() {

            @Override
            public void onChange() {
                System.out.println("TopologyFromZKTest.testAssignment().new TopologyChangeListener() {...}.onChange()");
                if (topologyFromZK.getTopology().getNodes().size() == 10) {
                    lock.lock();
                    try {
                        signal.signalAll();
                    } finally {
                        lock.unlock();
                    }

                }

            }
        };
        topologyFromZK.addListener(listener);
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            Runnable runnable = new Runnable() {

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
        boolean success = false;
        lock.lock();
        try {
            success = signal.await(3, TimeUnit.SECONDS);
        } finally {
            lock.unlock();
        }
        assertEquals(true, success);
        assertEquals(10, topologyFromZK.getTopology().getNodes().size());

    }
}
