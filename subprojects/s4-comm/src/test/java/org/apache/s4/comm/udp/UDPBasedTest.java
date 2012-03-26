package org.apache.s4.comm.udp;

import org.apache.s4.comm.util.ProtocolTestUtil;
import org.apache.s4.fixtures.ZkBasedClusterManagementTestModule;
import org.junit.Assert;

import com.google.inject.Guice;
import com.google.inject.name.Names;

public abstract class UDPBasedTest extends ProtocolTestUtil {
    protected UDPBasedTest() {
        super();
        super.injector = Guice.createInjector(new UDPTestModule());
    }

    protected UDPBasedTest(int numTasks) {
        super(numTasks);
        super.injector = Guice.createInjector(new UDPTestModule());
    }

    class UDPTestModule extends ZkBasedClusterManagementTestModule {
        UDPTestModule() {
            super(UDPEmitter.class, UDPListener.class);
        }

        @Override
        protected void configure() {
            super.configure();
            bind(Integer.class).annotatedWith(Names.named("emitter.send.interval")).toInstance(2);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.numMessages")).toInstance(250);
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
            messageDelivery();
        } catch (Exception e) {
            Assert.fail("UDP DeliveryTest");
        }
    }
}
