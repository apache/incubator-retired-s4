package org.apache.s4.comm.udp;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.MockReceiverModule;
import org.apache.s4.fixtures.NoOpReceiverModule;
import org.apache.s4.fixtures.TestCommModule;
import org.apache.s4.fixtures.UDPTransportModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class UDPBasicTest extends ZkBasedTest {

    public UDPBasicTest() {
        super(2);
    }

    @Test
    public void testSingleMessage() throws Exception {

        Injector injector1 = Guice.createInjector(Modules.override(
                new TestCommModule(Resources.getResource("default.s4.comm.properties").openStream())).with(
                new UDPTransportModule(), new NoOpReceiverModule()));
        // this picks partition 0
        Emitter emitter = injector1.getInstance(Emitter.class);

        Injector injector2 = Guice.createInjector(Modules.override(
                new TestCommModule(Resources.getResource("default.s4.comm.properties").openStream())).with(
                new UDPTransportModule(), new MockReceiverModule()));

        // creating the listener will inject assignment (i.e. assign a partition) and receiver (delegatee for
        // listener)
        injector2.getInstance(Listener.class);

        // send to the other partition (1)
        emitter.send(new UDPDestination(injector2.getInstance(Assignment.class).assignClusterNode()), injector1
                .getInstance(SerializerDeserializer.class).serialize(CommTestUtils.MESSAGE));

        Assert.assertTrue(CommTestUtils.SIGNAL_MESSAGE_RECEIVED.await(5, TimeUnit.SECONDS));

    }
}
