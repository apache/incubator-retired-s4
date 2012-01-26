package org.apache.s4.comm.udp;

import org.apache.s4.comm.util.ProtocolTestUtil;
import org.apache.s4.fixtures.ZkBasedClusterManagementTestModule;

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
            bind(Integer.class).annotatedWith(Names.named("emitter.send.interval")).toInstance(100);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.numMessages")).toInstance(200);
        }
    }
}
