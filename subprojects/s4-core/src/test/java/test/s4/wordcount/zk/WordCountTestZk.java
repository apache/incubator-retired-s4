package test.s4.wordcount.zk;

import static org.junit.Assert.*;
import static test.s4.wordcount.WordCountTest.*;
import java.io.File;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.core.App;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;

import test.s4.fixtures.TestUtils;
import test.s4.wordcount.WordCountApp;
import test.s4.wordcount.WordCountModule;

public class WordCountTestZk {

    private ZkServer zkServer;
    private ZkClient zkClient;

    @Before
    public void prepare() {

        String dataDir = TestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "zookeeper" + File.separator + "data";
        String logDir = TestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "zookeeper" + File.separator + "logs";
        TestUtils.cleanupTmpDirs();

        IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {

            }
        };

        zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, TestUtils.ZK_PORT);
        zkServer.start();

        // zkClient = zkServer.getZkClient();
        String zookeeperAddress = "localhost:" + TestUtils.ZK_PORT;
        zkClient = new ZkClient(zookeeperAddress, 10000, 10000);

        ZkClient zkClient2 = new ZkClient(zookeeperAddress, 10000, 10000);
        zkClient2.getCreationTime("/");
        TaskSetup taskSetup = new TaskSetup(zookeeperAddress);
        final String clusterName = "s4-test-cluster";
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 1);
        // final CountDownLatch latch = new CountDownLatch(10);
        // for (int i = 0; i < 10; i++) {
        // Runnable runnable = new Runnable() {
        //
        // @Override
        // public void run() {
        // AssignmentFromZK assignmentFromZK;
        // try {
        // assignmentFromZK = new AssignmentFromZK(clusterName, zookeeperAddress, 30000, 30000);
        // ClusterNode assignClusterNode = assignmentFromZK.assignClusterNode();
        // latch.countDown();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // }
        // };
        // Thread t = new Thread(runnable);
        // t.start();
        // }
    }

    @Test
    public void test() throws Exception {

        final ZooKeeper zk = TestUtils.createZkClient();

        App.main(new String[] { WordCountModuleZk.class.getName(), WordCountApp.class.getName() });

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed, zk);

        // add authorizations for processing
        for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + 1; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_1);
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_2);
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_3);
        signalTextProcessed.await();
        File results = new File(TestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", s);

    }
}
