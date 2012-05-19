package org.apache.s4.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.core.App;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class WordCountTest {

    public static final String SENTENCE_1 = "to be or not to be doobie doobie da";
    public static final int SENTENCE_1_TOTAL_WORDS = SENTENCE_1.split(" ").length;
    public static final String SENTENCE_2 = "doobie doobie da";
    public static final int SENTENCE_2_TOTAL_WORDS = SENTENCE_2.split(" ").length;
    public static final String SENTENCE_3 = "doobie";
    public static final int SENTENCE_3_TOTAL_WORDS = SENTENCE_3.split(" ").length;
    public static final String FLAG = ";";
    public static int TOTAL_WORDS = SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + SENTENCE_3_TOTAL_WORDS;
    private static Factory zookeeperServerConnectionFactory;

    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        CommTestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = CommTestUtils.startZookeeperServer();

    }

    /**
     * A simple word count application:
     * 
     * 
     * 
     * 
     * sentences words word counts Adapter ------------> WordSplitterPE -----------> WordCounterPE ------------->
     * WordClassifierPE key = "sentence" key = word key="classifier" (should be *)
     * 
     * 
     * The test consists in checking that words are correctly counted.
     * 
     * 
     */
    @Test
    public void testSimple() throws Exception {
        final ZooKeeper zk = CommTestUtils.createZkClient();
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(Resources.newInputStreamSupplier(Resources.getResource("default.s4.properties")).getInput());
        taskSetup.clean("s4");
        taskSetup.setup(config.getString("cluster.name"), 1, 10000);

        App.main(new String[] { WordCountModule.class.getName(), WordCountApp.class.getName() });

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed, zk);

        // add authorizations for processing
        for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + 1; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        CommTestUtils.injectIntoStringSocketAdapter(SENTENCE_1);
        CommTestUtils.injectIntoStringSocketAdapter(SENTENCE_2);
        CommTestUtils.injectIntoStringSocketAdapter(SENTENCE_3);
        signalTextProcessed.await();
        File results = new File(CommTestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "wordcount");
        String s = CommTestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", s);

    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        CommTestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);

    }

}
