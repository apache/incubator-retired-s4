package org.apache.s4.fixtures;

import java.io.File;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.s4.comm.tools.TaskSetup;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkBasedTest.class);
    private ZkServer zkServer;
    protected String zookeeperAddress = "localhost:" + CommTestUtils.ZK_PORT;

    private static final String clusterName = "s4-test-cluster";
    protected final int numTasks;

    protected ZkBasedTest() {
        this.numTasks = 1;
    }

    protected ZkBasedTest(int numTasks) {
        this.numTasks = numTasks;
    }

    @Before
    public void setupZkBasedTest() {
        String dataDir = CommTestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "zookeeper" + File.separator + "data";
        String logDir = CommTestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "zookeeper" + File.separator + "logs";
        CommTestUtils.cleanupTmpDirs();

        IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {

            }
        };

        logger.info("Starting Zookeeper Server");
        zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, CommTestUtils.ZK_PORT);
        zkServer.start();

        TaskSetup taskSetup = new TaskSetup(zookeeperAddress);
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, this.numTasks);
    }

    @After
    public void cleanupZkBasedTest() {
        if (zkServer != null) {
            zkServer.shutdown();
            zkServer = null;
        }
        CommTestUtils.cleanupTmpDirs();
    }
}
