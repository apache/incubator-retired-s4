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

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.AppModule;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.apache.s4.core.ft.FileSystemBasedBackendWithZKStorageCallbackCheckpointingModule.DummyZKStorageCallbackFactory;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.MockCommModule;
import org.apache.s4.fixtures.MockCoreModule;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class CheckpointingTest {

    private static Factory zookeeperServerConnectionFactory = null;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
            + "storage");

    @Before
    public void prepare() throws Exception {
        zookeeperServerConnectionFactory = CoreTestUtils.startZookeeperServer();
    }

    @After
    public void cleanup() throws Exception {
        CoreTestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
    }

    @Test
    public void testCheckpointStorage() throws Exception {
        final ZooKeeper zk = CoreTestUtils.createZkClient();

        // 2. generate a simple event that creates and changes the state of
        // the
        // PE

        // NOTE: coordinate through zookeeper
        final CountDownLatch signalValue1Set = new CountDownLatch(1);

        CoreTestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);
        final CountDownLatch signalCheckpointed = new CountDownLatch(1);
        CoreTestUtils.watchAndSignalCreation("/checkpointed", signalCheckpointed, zk);

        Injector injector = Guice.createInjector(new MockCommModule(),
                new MockCoreModuleWithFileBaseCheckpointingBackend(), new AppModule(getClass().getClassLoader()));
        TestApp app = injector.getInstance(TestApp.class);
        app.init();
        app.start();

        Event event = new Event();
        event.setStreamId("stream1");
        event.put("command", String.class, "setValue1");
        event.put("value", String.class, "message1");

        app.testStream.receiveEvent(event);

        signalValue1Set.await();

        StatefulTestPE pe = (StatefulTestPE) app.getPE("statefulPE1").getInstanceForKey("X");

        Assert.assertEquals("message1", pe.getValue1());
        Assert.assertEquals("", pe.getValue2());

        // 3. generate a checkpoint event
        event = new Event();
        event.setStreamId("stream1");
        event.put("command", String.class, "checkpoint");
        app.testStream.receiveEvent(event);
        Assert.assertTrue(signalCheckpointed.await(10, TimeUnit.SECONDS));

        // NOTE: the backend has asynchronous save operations
        Thread.sleep(1000);

        CheckpointId safeKeeperId = new CheckpointId(pe);
        File expected = new File(System.getProperty("user.dir") + File.separator + "tmp" + File.separator + "storage"
                + File.separator + safeKeeperId.getPrototypeId() + File.separator
                + Base64.encodeBase64URLSafeString(safeKeeperId.getStringRepresentation().getBytes()));

        // 4. verify that state was correctly persisted
        Assert.assertTrue(expected.exists());

        StatefulTestPE refPE = new StatefulTestPE();
        refPE.onCreate();
        refPE.setValue1("message1");

        Field idField = ProcessingElement.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(refPE, "X");

        byte[] refBytes = app.getSerDeser().serialize(refPE).array();

        Assert.assertTrue(Arrays.equals(refBytes, Files.toByteArray(expected)));

    }

    private static class TestApp extends App {
        Stream<Event> testStream;
        int count;

        @Override
        protected void onStart() {
        }

        @Override
        protected void onInit() {

            StatefulTestPE pe = createPE(StatefulTestPE.class, "statefulPE1");
            testStream = createStream("stream1", new KeyFinder<Event>() {
                @Override
                public List<String> get(Event event) {
                    return ImmutableList.of("X");
                }
            }, pe);
        }

        @Override
        protected void onClose() {
        }

    }

    private static class MockCoreModuleWithFileBaseCheckpointingBackend extends MockCoreModule {

        @Override
        protected void configure() {
            super.configure();
            bind(String.class).annotatedWith(Names.named("s4.checkpointing.filesystem.storageRootPath")).toInstance(
                    DEFAULT_STORAGE_DIR.getAbsolutePath());
            bind(StateStorage.class).to(DefaultFileSystemStateStorage.class);
            bind(CheckpointingFramework.class).to(SafeKeeper.class);
            bind(StorageCallbackFactory.class).to(DummyZKStorageCallbackFactory.class);

        }

    }

}
