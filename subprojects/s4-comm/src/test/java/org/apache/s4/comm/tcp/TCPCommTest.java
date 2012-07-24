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

import java.io.IOException;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.DeliveryTestUtil;
import org.apache.s4.comm.util.ProtocolTestUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public abstract class TCPCommTest extends ProtocolTestUtil {

    private static Logger logger = LoggerFactory.getLogger(TCPCommTest.class);
    DeliveryTestUtil util;
    public final static String CLUSTER_NAME = "cluster1";

    public TCPCommTest() throws IOException {
        super();
    }

    public TCPCommTest(int numTasks) throws IOException {
        super(numTasks);
    }

    public Injector newInjector() {
        try {
            return Guice.createInjector(new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                    .openStream(), CLUSTER_NAME));
        } catch (IOException e) {
            Assert.fail();
            return null;
        }
    }

    @Override
    public void testDelivery() throws InterruptedException {
        startThreads();
        waitForThreads();

        Assert.assertTrue("Message Delivery", messageDelivery());

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
