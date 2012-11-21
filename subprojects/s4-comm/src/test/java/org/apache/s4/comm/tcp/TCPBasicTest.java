package org.apache.s4.comm.tcp;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.MockReceiverModule;
import org.apache.s4.fixtures.NoOpReceiverModule;
import org.apache.s4.fixtures.TCPTransportModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TCPBasicTest extends ZkBasedTest {

    @Test
    public void testSingleMessage() {

        try {
            Injector injector1 = Guice
                    .createInjector(new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                            .openStream(), "cluster1"), new TCPTransportModule(), new NoOpReceiverModule());
            Emitter emitter = injector1.getInstance(Emitter.class);

            Injector injector2 = Guice
                    .createInjector(new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                            .openStream(), "cluster1"), new TCPTransportModule(), new MockReceiverModule());
            // creating the listener will inject assignment (i.e. assign a partition) and receiver (delegatee for
            // listener, here a mock which simply intercepts the message and notifies through a countdown latch)
            injector2.getInstance(Listener.class);

            emitter.send(0, injector1.getInstance(SerializerDeserializer.class).serialize(CommTestUtils.MESSAGE));

            // check receiver got the message
            Assert.assertTrue(CommTestUtils.SIGNAL_MESSAGE_RECEIVED.await(5, TimeUnit.SECONDS));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
