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

package org.apache.s4.core.ft;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Event;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.wordcount.WordCountTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
        // TODO replace with zk-notified-latch
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
        // TODO replace with zk-notified-latch
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
        Assert.assertTrue(signalTextProcessed.await(40, TimeUnit.SECONDS));
        String results = new String(zk.getData("/results", false, null));
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);

    }

    private void injectSentence(Injector injector, TCPEmitter emitter, String sentence) throws InterruptedException {
        Event event;
        event = new Event();
        event.setStreamId("inputStream");
        event.put("sentence", String.class, sentence);
        emitter.send(
                0,
                injector.getInstance(SerializerDeserializerFactory.class)
                        .createSerializerDeserializer(Thread.currentThread().getContextClassLoader()).serialize(event));
    }

    private void restartNode() throws IOException, InterruptedException {
        CountDownLatch signalConsumerReady = RecoveryTest.getConsumerReadySignal("inputStream");

        DeploymentUtils.initAppConfig(
                new AppConfig.Builder()
                        .appClassName(FTWordCountApp.class.getName())
                        .namedParameters(
                                ImmutableMap.of("s4.checkpointing.filesystem.storageRootPath",
                                        CommTestUtils.DEFAULT_STORAGE_DIR.getAbsolutePath(),
                                        "s4.checkpointing.storageMaxThreads", "3"))
                        .customModulesNames(ImmutableList.of(FileSystemBackendCheckpointingModule.class.getName()))
                        .build(), "cluster1", false, "localhost:2181");
        // recovering and making sure checkpointing still works
        forkedS4App = CoreTestUtils.forkS4Node(new String[] { "-c", "cluster1" }, new ZkClient("localhost:2181"), 10,
                "cluster1");
        Assert.assertTrue(signalConsumerReady.await(20, TimeUnit.SECONDS));
    }
}
