package org.apache.s4.core.moduleloader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.BaseModule;
import org.apache.s4.core.S4Node;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.wordcount.WordCountApp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestModuleLoader extends ZkBasedTest {

    private static final int NB_MESSAGES = 10;
    private Process forkS4Node;
    private TCPEmitter emitter;
    private Injector injector;

    public TestModuleLoader() {
        super(2);
    }

    @Test
    public void testLocal() throws Exception {
        testModuleLoader(false);
    }

    protected void testModuleLoader(boolean fork) throws Exception {

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
            forkS4Node = CoreTestUtils.forkS4Node(new String[] { "-c", "cluster1" });
        } else {
            S4Node.main(new String[] { "-c", "cluster1" });
        }

        // injector = Guice.createInjector(new BaseModule(
        // Resources.getResource("default.s4.base.properties").openStream(), "cluster1"),
        // new CommModuleWithoutListener(Resources.getResource("default.s4.comm.properties").openStream()));

        injector = Guice.createInjector(new BaseModule(
                Resources.getResource("default.s4.base.properties").openStream(), "cluster1"), new DefaultCommModule(
                Resources.getResource("default.s4.comm.properties").openStream()));

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

        for (Long message : messages) {
            Event event = new Event();
            event.put("message", long.class, message);
            emitter.send(0, new EventMessage("-1", "inputStream", injector.getInstance(SerializerDeserializer.class)
                    .serialize(event)));
        }

        // check sequential nodes in zk with correct data
        Assert.assertTrue(signalMessagesReceived.await(10, TimeUnit.SECONDS));
        List<String> children = zkClient.getChildren("/test");
        for (String child : children) {
            Long data = zkClient.readData("/test/" + child);
            Assert.assertTrue(messages.contains(data));
        }

    }

    @After
    public void cleanUp() throws IOException, InterruptedException {
        CoreTestUtils.killS4App(forkS4Node);
    }

}
