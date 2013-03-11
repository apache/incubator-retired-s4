/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.core.ri;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.wordcount.IsolationWordCounterPE;
import org.apache.s4.wordcount.SentenceKeyFinder;
import org.apache.s4.wordcount.WordClassifierPE;
import org.apache.s4.wordcount.WordCountEvent;
import org.apache.s4.wordcount.WordCountKeyFinder;
import org.apache.s4.wordcount.WordCountModule;
import org.apache.s4.wordcount.WordCountTest;
import org.apache.s4.wordcount.WordSeenEvent;
import org.apache.s4.wordcount.WordSeenKeyFinder;
import org.apache.s4.wordcount.WordSplitterPE;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class RuntimeIsolationTest extends WordCountTest {
    private static Logger logger = LoggerFactory.getLogger(RuntimeIsolationTest.class);

    final static int numberTasks = 5;
    protected Process[] s4nodes;

    protected static int counterNumber = 2;

    public RuntimeIsolationTest() {
        super(numberTasks);
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        if (s4nodes == null) {
            return;
        }

        for (Process s4node : s4nodes) {
            CoreTestUtils.killS4App(s4node);
        }
    }

    @Test
    public void testSimple() {
        try {
            final ZooKeeper zk = CommTestUtils.createZkClient();

            zk.create("/counters", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            // start nodes
            startNodes();

            // we create the emitter now, it will share zk node assignment with the S4 node
            createEmitter();

            CountDownLatch signalTextProcessed = new CountDownLatch(1);
            CommTestUtils.watchAndSignalCreation("/results", signalTextProcessed, zk);

            // add authorizations for processing
            for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + 1; i++) {
                zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }

            injectData();

            Assert.assertTrue(signalTextProcessed.await(30, TimeUnit.SECONDS));
            String results = new String(zk.getData("/results", false, null));
            Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);

            List<String> counterInstances = zk.getChildren("/counters", false);

            int totalCount = 0;
            int activeInstances = 0;
            for (String instance : counterInstances) {
                int count = Integer.parseInt(new String(zk.getData("/counters/" + instance, false, null)));
                if (count != 0) {
                    activeInstances++;
                }
                totalCount += count;
            }
            Assert.assertEquals(numberTasks, counterInstances.size());
            Assert.assertEquals(counterNumber, activeInstances);

            Assert.assertEquals(13, totalCount);
        } catch (Exception e) {
            logger.error("ERROR!", e);
        }
    }

    public void injectData() throws IOException, InterruptedException {
        injectSentence(SENTENCE_1);
        injectSentence(SENTENCE_2);
        injectSentence(SENTENCE_3);
    }

    public void startNodes() throws InterruptedException, IOException {
        s4nodes = new Process[numberTasks];
        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(IsolationWordCountApp.class.getName())
                .customModulesNames(ImmutableList.of(WordCountModule.class.getName())).build(), "cluster1", false,
                "localhost:2181");
        s4nodes = CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster1" }, new ZkClient("localhost:2181"), 10,
                "cluster1", numberTasks);
    }

    static class IsolationWordCountApp extends App {

        protected boolean checkpointing = false;

        @Inject
        public IsolationWordCountApp() {
            super();
        }

        @Override
        protected void onStart() {
        }

        @Override
        protected void onInit() {

            WordClassifierPE wordClassifierPrototype = createPE(WordClassifierPE.class, "classifierPE");
            // set WordClassifierPE has 2 exclusive partitions
//            wordClassifierPrototype.setExclusive(2);

            Stream<WordCountEvent> wordCountStream = createStream("words counts stream", new WordCountKeyFinder(),
                    wordClassifierPrototype);
            IsolationWordCounterPE wordCounterPrototype = createPE(IsolationWordCounterPE.class, "counterPE");
            wordCounterPrototype.setExclusive(counterNumber);

            wordCounterPrototype.setWordClassifierStream(wordCountStream);
            Stream<WordSeenEvent> wordSeenStream = createStream("words seen stream", new WordSeenKeyFinder(),
                    wordCounterPrototype);
            WordSplitterPE wordSplitterPrototype = createPE(WordSplitterPE.class);
            wordSplitterPrototype.setWordSeenStream(wordSeenStream);
            Stream<Event> sentenceStream = createInputStream("inputStream", new SentenceKeyFinder(),
                    wordSplitterPrototype);

        }

        @Override
        protected void onClose() {

        }

    }

}
