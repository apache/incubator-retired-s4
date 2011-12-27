package org.apache.s4.wordcount.zk;

import static org.apache.s4.wordcount.WordCountTest.*;
import java.io.File;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.s4.core.App;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.wordcount.WordCountApp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import org.junit.Test;

public class WordCountTestZk extends ZkBasedTest {
    @Test
    public void test() throws Exception {

        final ZooKeeper zk = CommTestUtils.createZkClient();

        App.main(new String[] { WordCountModuleZk.class.getName(), WordCountApp.class.getName() });

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
}
