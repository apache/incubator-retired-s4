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

package org.apache.s4.comm.util;

import java.io.IOException;

import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;

public abstract class ProtocolTestUtil extends ZkBasedTest {
    protected int[] expectedMessages;
    protected PartitionInfo[] partitions;

    protected ProtocolTestUtil() {
        super();
    }

    protected ProtocolTestUtil(int numTasks) {
        super(numTasks);
    }

    @Before
    public void preparePartitions() throws IOException, InterruptedException, KeeperException {
        expectedMessages = new int[super.numTasks];
        partitions = new PartitionInfo[super.numTasks];
        for (int i = 0; i < this.numTasks; i++) {
            partitions[i] = newInjector().getInstance(PartitionInfo.class);
            partitions[i].setProtocolTestUtil(this);
        }
    }

    protected abstract Injector newInjector() throws IOException;

    protected void decreaseExpectedMessages(int partition, long amount) {
        synchronized (expectedMessages) {
            expectedMessages[partition] -= amount;
        }

        if (partitions[partition].receiveThread.messagesReceived >= expectedMessages[partition])
            interrupt(partition);
    }

    protected void interrupt(int partition) {
        partitions[partition].receiveThread.interrupt();
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

    protected boolean messageDelivery() {
        for (PartitionInfo partition : partitions) {
            if (!partition.receiveThread.messageDelivery())
                return false;
        }
        return true;
    }

    protected boolean messageOrdering() {
        for (PartitionInfo partition : partitions) {
            if (!partition.receiveThread.orderedDelivery())
                return false;
        }
        return true;
    }

    @After
    public void tearDown() {
        for (PartitionInfo partition : partitions) {
            // debug
            partition.receiveThread.printCounts();
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

    @Test(timeout = 60000)
    public abstract void testDelivery() throws InterruptedException;
}
