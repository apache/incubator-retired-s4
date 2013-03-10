package org.apache.s4.fixtures;

import java.io.InputStream;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;

import com.google.inject.name.Names;

/**
 * Binds dependencies that come for the base layer and are defined in {@link BaseModule} in s4-core.
 * 
 * We need them injected for the tests to work, in particular for getting an assignment
 * 
 * 
 */
public class TestCommModule extends DefaultCommModule {

    public TestCommModule(InputStream commConfigInputStream) {
        super(commConfigInputStream);
    }

    @Override
    protected void configure() {
        super.configure();
        bind(String.class).annotatedWith(Names.named("s4.cluster.name")).toInstance("cluster1");
        bind(String.class).annotatedWith(Names.named("s4.cluster.zk_address")).toInstance("localhost:2181");
        bind(Integer.class).annotatedWith(Names.named("s4.cluster.zk_session_timeout")).toInstance(10000);
        bind(Integer.class).annotatedWith(Names.named("s4.cluster.zk_connection_timeout")).toInstance(10000);
        bind(Assignment.class).to(AssignmentFromZK.class).asEagerSingleton();

        ZkClient zkClient = new ZkClient(CommTestUtils.ZK_STRING);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        bind(ZkClient.class).toInstance(zkClient);
    }
}
