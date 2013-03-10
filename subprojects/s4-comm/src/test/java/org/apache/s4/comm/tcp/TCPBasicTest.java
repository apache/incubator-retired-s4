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
package org.apache.s4.comm.tcp;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.MockReceiverModule;
import org.apache.s4.fixtures.NoOpReceiverModule;
import org.apache.s4.fixtures.TCPTransportModule;
import org.apache.s4.fixtures.TestCommModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class TCPBasicTest extends ZkBasedTest {

    public TCPBasicTest() {
        super(2);
    }

    @Test
    public void testSingleMessage() throws Exception {

        Injector injector1 = Guice.createInjector(Modules.override(
                new TestCommModule(Resources.getResource("default.s4.comm.properties").openStream())).with(
                new TCPTransportModule(), new NoOpReceiverModule()));

        // this node picks partition 0
        Emitter emitter = injector1.getInstance(Emitter.class);

        Injector injector2 = Guice.createInjector(Modules.override(
                new TestCommModule(Resources.getResource("default.s4.comm.properties").openStream())).with(
                new TCPTransportModule(), new MockReceiverModule()));

        // creating the listener will inject assignment (i.e. assign a partition) and receiver (delegatee for
        // listener, here a mock which simply intercepts the message and notifies through a countdown latch)
        injector2.getInstance(Listener.class);

        // send to the other node
        emitter.send(1, injector1.getInstance(SerializerDeserializer.class).serialize(CommTestUtils.MESSAGE));

        // check receiver got the message
        Assert.assertTrue(CommTestUtils.SIGNAL_MESSAGE_RECEIVED.await(5, TimeUnit.SECONDS));

    }
}
