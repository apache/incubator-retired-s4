package org.apache.s4.comm.topology;

import java.io.IOException;

import org.apache.s4.fixtures.CommTestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;

public class ZKBaseTest {

    private Factory zkFactory;

    @Before
    public void setUp() throws IOException, InterruptedException, KeeperException {
        CommTestUtils.cleanupTmpDirs();
        zkFactory = CommTestUtils.startZookeeperServer();

    }

    @After
    public void tearDown() throws Exception {
        CommTestUtils.stopZookeeperServer(zkFactory);
    }
}
