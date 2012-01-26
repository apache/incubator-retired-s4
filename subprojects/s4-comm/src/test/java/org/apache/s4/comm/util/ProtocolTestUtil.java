package org.apache.s4.comm.util;

import java.io.IOException;

import org.apache.s4.comm.util.PartitionInfo;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

import com.google.inject.Injector;

public abstract class ProtocolTestUtil extends ZkBasedTest {
    protected PartitionInfo[] partitions;
    protected Injector injector;

    protected ProtocolTestUtil() {
        super();
    }

    protected ProtocolTestUtil(int numTasks) {
        super(numTasks);
    }

    @Before
    public void setup() throws IOException, InterruptedException, KeeperException {
        partitions = new PartitionInfo[super.numTasks];
        for (int i = 0; i < this.numTasks; i++) {
            partitions[i] = injector.getInstance(PartitionInfo.class);
        }
    }

    protected void startThreads() {
        for (PartitionInfo partition : partitions) {
            partition.sendThread.start();
            partition.receiveThread.start();
        }
    }

    protected void waitForThreads() throws InterruptedException {
        for (PartitionInfo partition : partitions) {
            partition.sendThread.join();
            partition.receiveThread.join();
        }
    }

    protected int notDeliveredMessages() {
        int sum = 0;
        for (PartitionInfo partition : partitions) {
            sum += partition.receiveThread.moreMessages();
        }
        return sum;
    }

    protected boolean messageOrdering() {
        for (PartitionInfo partition : partitions) {
            if (!partition.receiveThread.ordered())
                return false;
        }
        return true;
    }

    @After
    public void tearDown() {
        for (PartitionInfo partition : partitions) {
            if (partition.emitter != null) {
                partition.emitter.close();
                partition.emitter = null;
            }
            if (partition.listener != null) {
                partition.listener.close();
                partition.listener = null;
            }
        }
    }
}
