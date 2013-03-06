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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Event;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.s4.core.S4Node;
import org.apache.s4.core.Stream;
import org.apache.s4.core.ft.FTWordCountApp;
import org.apache.s4.core.ft.FileSystemBackendCheckpointingModule;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.wordcount.SentenceKeyFinder;
import org.apache.s4.wordcount.WordClassifierPE;
import org.apache.s4.wordcount.WordCountEvent;
import org.apache.s4.wordcount.WordCountKeyFinder;
import org.apache.s4.wordcount.WordCountModule;
import org.apache.s4.wordcount.WordCountTest;
import org.apache.s4.wordcount.WordCounterPE;
import org.apache.s4.wordcount.WordSeenEvent;
import org.apache.s4.wordcount.WordSeenKeyFinder;
import org.apache.s4.wordcount.WordSplitterPE;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

public class RuntimeIsolationTest extends WordCountTest {

    final static int numberTasks = 5;
    private Process[] s4nodes;

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

    /**
     * reuse {@link WordCountTest}. Start 3 nodes.
     * 
     */
    @Test
    @Override
    public void testSimple() throws Exception {
        final ZooKeeper zk = CommTestUtils.createZkClient();

        // start 3 nodes
        startNodes("cluster1", IsolationWordCountApp.class.getName(), numberTasks);

        // we create the emitter now, it will share zk node assignment with the S4 node
        createEmitter();

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation("/results", signalTextProcessed, zk);

        // add authorizations for processing
        for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + 1; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        injectSentence(SENTENCE_1);
        injectSentence(SENTENCE_2);
        injectSentence(SENTENCE_3);
        Assert.assertTrue(signalTextProcessed.await(30, TimeUnit.SECONDS));
        String results = new String(zk.getData("/results", false, null));
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);

    }

    public void startNodes(String clusterName, String appClass, int number) throws InterruptedException, IOException {
        s4nodes = new Process[number];
        DeploymentUtils.initAppConfig(
                new AppConfig.Builder()
                        .appClassName(appClass)
                        .customModulesNames(ImmutableList.of(WordCountModule.class.getName()))
                        .build(), "cluster1", false, "localhost:2181");
        s4nodes = CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster1" }, new ZkClient("localhost:2181"), 10,
                "cluster1", number);
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
            wordClassifierPrototype.setExclusive(2);

            Stream<WordCountEvent> wordCountStream = createStream("words counts stream", new WordCountKeyFinder(),
                    wordClassifierPrototype);
            WordCounterPE wordCounterPrototype = createPE(WordCounterPE.class, "counterPE");
            wordCounterPrototype.setExclusive(2);

            wordCounterPrototype.setWordClassifierStream(wordCountStream);
            Stream<WordSeenEvent> wordSeenStream = createStream("words seen stream", new WordSeenKeyFinder(),
                    wordCounterPrototype);
            WordSplitterPE wordSplitterPrototype = createPE(WordSplitterPE.class);
            wordSplitterPrototype.setWordSeenStream(wordSeenStream);
            Stream<Event> sentenceStream = createInputStream("inputStream", new SentenceKeyFinder(),
                    wordSplitterPrototype);
            
            //TestPE
            WordSplitterPE wordSplitterTestPE = createPE(WordSplitterPE.class,"TestPE");
        }

        @Override
        protected void onClose() {

        }

    }
}
