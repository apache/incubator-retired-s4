package org.apache.s4.fixtures;

import java.io.IOException;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkBasedTest.class);
    private Factory zkFactory;

    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        CommTestUtils.cleanupTmpDirs();

        zkFactory = CommTestUtils.startZookeeperServer();

        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean("s4");
        taskSetup.setup("cluster1", 1, 1300);
    }

    @After
    public void cleanupZkBasedTest() throws IOException, InterruptedException {
        CommTestUtils.stopZookeeperServer(zkFactory);
        CommTestUtils.cleanupTmpDirs();
    }
}
