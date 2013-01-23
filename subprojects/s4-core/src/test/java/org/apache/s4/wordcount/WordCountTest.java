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

package org.apache.s4.wordcount;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Event;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.core.BaseModule;
import org.apache.s4.core.DefaultCoreModule;
import org.apache.s4.core.S4Node;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class WordCountTest extends ZkBasedTest {

    public static final String SENTENCE_1 = "to be or not to be doobie doobie da";
    public static final int SENTENCE_1_TOTAL_WORDS = SENTENCE_1.split(" ").length;
    public static final String SENTENCE_2 = "doobie doobie da";
    public static final int SENTENCE_2_TOTAL_WORDS = SENTENCE_2.split(" ").length;
    public static final String SENTENCE_3 = "doobie";
    public static final int SENTENCE_3_TOTAL_WORDS = SENTENCE_3.split(" ").length;
    public static final String FLAG = ";";
    public static int TOTAL_WORDS = SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + SENTENCE_3_TOTAL_WORDS;
    private TCPEmitter emitter;
    Injector injector;

    public void createEmitter() throws IOException {
        injector = Guice.createInjector(new BaseModule(
                Resources.getResource("default.s4.base.properties").openStream(), "cluster1"), new DefaultCommModule(
                Resources.getResource("default.s4.comm.properties").openStream()), new DefaultCoreModule(Resources
                .getResource("default.s4.core.properties").openStream()));

        emitter = injector.getInstance(TCPEmitter.class);

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
        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(WordCountApp.class.getName()).build(),
                "cluster1", true, "localhost:2181");
        S4Node.main(new String[] { "-cluster=cluster1", });

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
        Assert.assertTrue(signalTextProcessed.await(10, TimeUnit.SECONDS));
        String results = new String(zk.getData("/results", false, null));
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);
    }

    public void injectSentence(String sentence) throws IOException, InterruptedException {
        Event event = new Event();
        event.setStreamId("inputStream");
        event.put("sentence", String.class, sentence);

        // NOTE: we send to partition 0 since partition 1 hosts the emitter
        emitter.send(
                0,
                injector.getInstance(SerializerDeserializerFactory.class)
                        .createSerializerDeserializer(Thread.currentThread().getContextClassLoader()).serialize(event));
    }

}
