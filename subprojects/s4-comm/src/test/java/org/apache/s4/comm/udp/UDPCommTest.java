package org.apache.s4.comm.udp;

import java.io.IOException;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.DeliveryTestUtil;
import org.apache.s4.comm.tcp.TCPCommTest;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class UDPCommTest extends ZkBasedTest {
    DeliveryTestUtil util;

    @Before
    public void setup() throws IOException, InterruptedException, KeeperException {
        Injector injector = Guice.createInjector(new DefaultCommModule(Resources.getResource("udp.s4.comm.properties")
                .openStream(), TCPCommTest.CLUSTER_NAME), new UDPCommTestModule());
        util = injector.getInstance(DeliveryTestUtil.class);
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
    @Test
    public void testUDPDelivery() throws InterruptedException {
        try {
            Thread sendThread = util.newSendThread();
            Thread receiveThread = util.newReceiveThread();

            // start send and receive threads
            sendThread.start();
            receiveThread.start();

            // wait for them to finish
            sendThread.join();
            receiveThread.join();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("UDP has failed basic functionality test");
        }
        System.out.println("Done");
    }
}
