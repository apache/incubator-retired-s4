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

package org.apache.s4.comm.udp;

import java.io.IOException;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.DeliveryTestUtil;
import org.apache.s4.comm.util.ProtocolTestUtil;
import org.junit.Assert;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public abstract class UDPCommTest extends ProtocolTestUtil {
    DeliveryTestUtil util;

    public UDPCommTest() throws IOException {
        super();
    }

    public UDPCommTest(int numTasks) throws IOException {
        super(numTasks);
    }

    @Override
    protected Injector newInjector() throws IOException {
        return Guice.createInjector(new DefaultCommModule(Resources.getResource("udp.s4.comm.properties").openStream(),
                "cluster1"), new UDPCommTestModule());
    }

    class UDPCommTestModule extends AbstractModule {
        UDPCommTestModule() {
        }

        @Override
        protected void configure() {
            bind(Integer.class).annotatedWith(Names.named("emitter.send.interval")).toInstance(100);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.numMessages")).toInstance(200);
            bind(Integer.class).annotatedWith(Names.named("listener.recv.sleepCount")).toInstance(10);
        }
    }

    /**
     * Tests the protocol. If all components function without throwing exceptions, the test passes.
     * 
     * @throws InterruptedException
     */
    @Override
    public void testDelivery() {
        try {
            Thread.sleep(1000);
            startThreads();
            waitForThreads();
            Assert.assertTrue("Message Delivery", messageDelivery());
        } catch (Exception e) {
            Assert.fail("UDP DeliveryTest");
        }
    }
}
