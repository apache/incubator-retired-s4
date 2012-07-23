package org.apache.s4.core.ft;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.wordcount.WordCountTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Test;

import com.google.inject.Injector;

public class FTWordCountTest extends ZkBasedTest {

    private Process forkedS4App;

    @After
    public void clean() throws IOException, InterruptedException {
        CoreTestUtils.killS4App(forkedS4App);
    }

    @Test
    public void testCheckpointAndRecovery() throws Exception {

        Injector injector = CoreTestUtils.createInjectorWithNonFailFastZKClients();

        TCPEmitter emitter = injector.getInstance(TCPEmitter.class);

        final ZooKeeper zk = CoreTestUtils.createZkClient();

        restartNode();

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/results", signalTextProcessed, zk);

        // add authorizations for processing
        for (int i = 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        CountDownLatch signalSentence1Processed = new CountDownLatch(1);
        CoreTestUtils.watchAndSignalCreation("/classifierIteration_" + WordCountTest.SENTENCE_1_TOTAL_WORDS,
                signalSentence1Processed, zk);

        injectSentence(injector, emitter, WordCountTest.SENTENCE_1);
        signalSentence1Processed.await(10, TimeUnit.SECONDS);
        Thread.sleep(1000);

        // crash the app
        forkedS4App.destroy();

        restartNode();
        // add authorizations for continuing processing. Without these, the
        // WordClassifier processed keeps waiting
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS + 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS
                + WordCountTest.SENTENCE_2_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        CountDownLatch sentence2Processed = new CountDownLatch(1);
        CoreTestUtils
                .watchAndSignalCreation("/classifierIteration_"
                        + (WordCountTest.SENTENCE_1_TOTAL_WORDS + WordCountTest.SENTENCE_2_TOTAL_WORDS),
                        sentence2Processed, zk);

        injectSentence(injector, emitter, WordCountTest.SENTENCE_2);

        sentence2Processed.await(10, TimeUnit.SECONDS);
        Thread.sleep(1000);

        // crash the app
        forkedS4App.destroy();
        restartNode();

        // add authorizations for continuing processing. Without these, the
        // WordClassifier processed keeps waiting
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS + WordCountTest.SENTENCE_2_TOTAL_WORDS + 1; i <= WordCountTest.TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        injectSentence(injector, emitter, WordCountTest.SENTENCE_3);
        Assert.assertTrue(signalTextProcessed.await(10, TimeUnit.SECONDS));
        String results = new String(zk.getData("/results", false, null));
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);

    }

    private void injectSentence(Injector injector, TCPEmitter emitter, String sentence) {
        Event event;
        event = new Event();
        event.put("sentence", String.class, sentence);
        emitter.send(0, new EventMessage("-1", "inputStream", injector.getInstance(SerializerDeserializer.class)
                .serialize(event)));
    }

    private void restartNode() throws IOException, InterruptedException {
        CountDownLatch signalConsumerReady = RecoveryTest.getConsumerReadySignal("inputStream");

        // recovering and making sure checkpointing still works
        forkedS4App = CoreTestUtils.forkS4Node(new String[] { "-c", "cluster1", "-appClass",
                FTWordCountApp.class.getName(), "-p",
                "s4.checkpointing.filesystem.storageRootPath=" + CommTestUtils.DEFAULT_STORAGE_DIR,
                "-extraModulesClasses", FileSystemBackendCheckpointingModule.class.getName() });
        Assert.assertTrue(signalConsumerReady.await(20, TimeUnit.SECONDS));
    }

}
