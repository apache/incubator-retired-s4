package org.apache.s4.comm.tcp;

import java.io.IOException;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.DeliveryTestUtil;
import org.apache.s4.comm.util.ProtocolTestUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public abstract class TCPCommTest extends ProtocolTestUtil {

    private static Logger logger = LoggerFactory.getLogger(TCPCommTest.class);
    DeliveryTestUtil util;
    public final static String CLUSTER_NAME = "cluster1";
    Injector injector;

    public TCPCommTest() throws IOException {
        super();
        injector = Guice.createInjector(new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                .openStream(), CLUSTER_NAME), new TCPCommTestModule());
    }

    public TCPCommTest(int numTasks) throws IOException {
        super(numTasks);
        injector = Guice.createInjector(new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                .openStream(), CLUSTER_NAME), new TCPCommTestModule());
    }

    public Injector getInjector() {
        return injector;
    }

    class TCPCommTestModule extends AbstractModule {
        TCPCommTestModule() {

        }

        @Override
        protected void configure() {
            bind(Integer.class).annotatedWith(Names.named("emitter.send.interval")).toInstance(100);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.numMessages")).toInstance(200);
            bind(Integer.class).annotatedWith(Names.named("listener.recv.sleepCount")).toInstance(10);
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
