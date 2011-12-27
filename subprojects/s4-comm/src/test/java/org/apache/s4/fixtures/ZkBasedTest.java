package org.apache.s4.fixtures;

import java.io.File;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.s4.comm.tools.TaskSetup;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkBasedTest.class);
    private ZkServer zkServer;

    @Before
    public void prepare() {
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

        logger.info("Starting Zookeeper Client 1");
        String zookeeperAddress = "localhost:" + CommTestUtils.ZK_PORT;
        @SuppressWarnings("unused")
        ZkClient zkClient = new ZkClient(zookeeperAddress, 10000, 10000);

        ZkClient zkClient2 = new ZkClient(zookeeperAddress, 10000, 10000);
        zkClient2.getCreationTime("/");

        TaskSetup taskSetup = new TaskSetup(zookeeperAddress);
        final String clusterName = "s4-test-cluster";
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 1);
    }
}
