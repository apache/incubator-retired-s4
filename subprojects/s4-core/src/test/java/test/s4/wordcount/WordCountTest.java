package test.s4.wordcount;


import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.s4.core.App;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.s4.fixtures.TestUtils;

public class WordCountTest {
    
    public static final String SENTENCE_1 = "to be or not to be doobie doobie da";
    public static final int SENTENCE_1_TOTAL_WORDS = SENTENCE_1.split(" ").length;
    public static final String SENTENCE_2 = "doobie doobie da";
    public static final int SENTENCE_2_TOTAL_WORDS = SENTENCE_2.split(" ").length;
    public static final String SENTENCE_3 = "doobie";
    public static final int SENTENCE_3_TOTAL_WORDS = SENTENCE_3.split(" ").length;
    public static final String FLAG = ";";
    public static int TOTAL_WORDS = SENTENCE_1_TOTAL_WORDS
            + SENTENCE_2_TOTAL_WORDS + SENTENCE_3_TOTAL_WORDS;
    private static Factory zookeeperServerConnectionFactory;
    
    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();

    }

    /**
     * A simple word count application:
     * 
     * 
     * 
     * 
     *           sentences                      words                    word counts
     * Adapter ------------> WordSplitterPE -----------> WordCounterPE -------------> WordClassifierPE 
     *                       key = "sentence"             key = word                   key="classifier"
     *                       (should be *)               
     * 
     * 
     * The test consists in checking that words are correctly counted.
     * 
     * 
     */
    @Test
    public void testSimple() throws Exception {
        
        final ZooKeeper zk = TestUtils.createZkClient();
        
        App.main(new String[]{WordCountModule.class.getName(), WordCountApp.class.getName()});
        

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed,
                zk);
        
        // add authorizations for processing
        for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS
                + 1; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_1);
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_2);
        TestUtils.injectIntoStringSocketAdapter(SENTENCE_3);
        signalTextProcessed.await();
        File results = new File(TestUtils.DEFAULT_TEST_OUTPUT_DIR
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", s);
        
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);

    }

}
