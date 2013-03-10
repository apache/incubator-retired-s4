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
package org.apache.s4.core.moduleloader;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Event;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.BaseModule;
import org.apache.s4.core.S4Node;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.wordcount.WordCountApp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class ModuleLoaderTestUtils {

    private static final int NB_MESSAGES = 10;

    public static Process testModuleLoader(boolean fork) throws Exception {

        Process forkedS4Node = null;
        // build custom-modules.jar
        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/custom-modules/build.gradle"), "clean", new String[0]);

        File modulesJarFile = new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/custom-modules/build/libs/app/custom-modules.jar");

        Assert.assertFalse(modulesJarFile.exists());

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/custom-modules/build.gradle"), "jar", new String[0]);

        // make sure it is created
        Assert.assertTrue(modulesJarFile.exists());

        // pass it as a configuration
        DeploymentUtils.initAppConfig(
                new AppConfig.Builder().appClassName(WordCountApp.class.getName())
                        .customModulesURIs(ImmutableList.of(modulesJarFile.toURI().toString()))
                        .customModulesNames(ImmutableList.of("org.apache.s4.TestListenerModule")).build(), "cluster1",
                true, "localhost:2181");
        if (fork) {
            forkedS4Node = CoreTestUtils.forkS4Node(new String[] { "-c", "cluster1" }, new ZkClient("localhost:2181"),
                    10, "cluster1");
        } else {
            S4Node.main(new String[] { "-c", "cluster1" });
        }

        Injector injector = Guice.createInjector(new BaseModule(Resources.getResource("default.s4.base.properties")
                .openStream(), "cluster1"), new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                .openStream()));

        Emitter emitter = injector.getInstance(TCPEmitter.class);
        List<Long> messages = Lists.newArrayList();
        for (int i = 0; i < NB_MESSAGES; i++) {
            messages.add(System.currentTimeMillis());
        }

        ZkClient zkClient = new ZkClient("localhost:2181");
        zkClient.create("/test", 0, CreateMode.PERSISTENT);

        final ZooKeeper zk = CommTestUtils.createZkClient();
        final CountDownLatch signalMessagesReceived = new CountDownLatch(1);

        // watch for last message in test data sequence
        CoreTestUtils.watchAndSignalCreation("/test/data" + Strings.padStart(String.valueOf(NB_MESSAGES - 1), 10, '0'),
                signalMessagesReceived, zk);

        SerializerDeserializer serDeser = injector.getInstance(SerializerDeserializerFactory.class)
                .createSerializerDeserializer(Thread.currentThread().getContextClassLoader());
        for (Long message : messages) {
            Event event = new Event();
            event.put("message", long.class, message);
            event.setStreamId("inputStream");
            emitter.send(0, serDeser.serialize(event));
        }

        // check sequential nodes in zk with correct data
        Assert.assertTrue(signalMessagesReceived.await(10, TimeUnit.SECONDS));
        List<String> children = zkClient.getChildren("/test");
        for (String child : children) {
            Long data = zkClient.readData("/test/" + child);
            Assert.assertTrue(messages.contains(data));
        }

        return forkedS4Node;

    }
}
