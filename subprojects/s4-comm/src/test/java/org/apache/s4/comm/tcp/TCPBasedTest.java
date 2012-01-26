package org.apache.s4.comm.tcp;

import org.apache.s4.comm.util.ProtocolTestUtil;
import org.apache.s4.fixtures.ZkBasedClusterManagementTestModule;

import com.google.inject.Guice;
import com.google.inject.name.Names;

public abstract class TCPBasedTest extends ProtocolTestUtil {
    protected TCPBasedTest() {
        super();
        super.injector = Guice.createInjector(new TCPTestModule());
    }

    protected TCPBasedTest(int numTasks) {
        super(numTasks);
        super.injector = Guice.createInjector(new TCPTestModule());
    }

    class TCPTestModule extends ZkBasedClusterManagementTestModule {
        TCPTestModule() {
            super(TCPEmitter.class, TCPListener.class);
        }

        @Override
        protected void configure() {
            super.configure();
            bind(Integer.class).annotatedWith(Names.named("tcp.partition.queue_size")).toInstance(256);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.interval")).toInstance(2);
            bind(Integer.class).annotatedWith(Names.named("emitter.send.numMessages")).toInstance(100);
        }
    }
}
