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

package org.apache.s4.core;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.core.triggers.TriggeredApp;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.MockCommModule;
import org.apache.s4.fixtures.MockCoreModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.wordcount.StringEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * tests from subclasses are forked in separate VMs, an easy way to avoid conflict with unavailable resources when
 * instantiating new S4 nodes
 */
// NOTE: placed in this package so that App#start(), init() and close() can be called without modifying their visibility
public abstract class TriggerTest extends ZkBasedTest {

    private Factory zookeeperServerConnectionFactory;
    public static TriggerType triggerType;
    protected TriggeredApp app;

    public enum TriggerType {
        TIME_BASED, COUNT_BASED, NONE
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        if (app != null) {
            app.close();
            app = null;
        }
        cleanupZkBasedTest();
    }

    protected CountDownLatch createTriggerAppAndSendEvent() throws IOException, KeeperException, InterruptedException {
        final ZooKeeper zk = CommTestUtils.createZkClient();
        Injector injector = Guice.createInjector(new MockCommModule(), new MockCoreModule(), new AppModule(getClass()
                .getClassLoader()));
        app = injector.getInstance(TriggeredApp.class);
        app.init();
        app.start();
        // app.close();

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, zk);

        CountDownLatch signalEvent1Triggered = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/onTrigger[StringEvent]@" + time1, signalEvent1Triggered, zk);

        StringEvent event = new StringEvent(time1);
        event.setStreamId("stream");
        app.stream.receiveEvent(event);

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));

        // return latch on trigger signal
        return signalEvent1Triggered;
    }
}
