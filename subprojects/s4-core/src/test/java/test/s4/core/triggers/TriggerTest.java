package test.s4.core.triggers;


import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;

import test.s4.fixtures.TestUtils;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * tests from subclasses are forked in separate VMs, an easy way to avoid
 * conflict with unavailable resources when instantiating new S4 nodes
 */

public abstract class TriggerTest {

    private Factory zookeeperServerConnectionFactory;
    public static TriggerType triggerType;
    protected TriggeredApp app;

    public enum TriggerType {
        TIME_BASED, COUNT_BASED, NONE
    }

    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        if (app != null) {
            app.close();
            app = null;
        }
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
    }

    protected CountDownLatch createTriggerAppAndSendEvent() throws IOException, KeeperException, InterruptedException {
        final ZooKeeper zk = TestUtils.createZkClient();
        Injector injector = Guice.createInjector(new TriggeredModule());
        app = injector.getInstance(TriggeredApp.class);
        app.init();
        app.start();
        
        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, zk);
        
        CountDownLatch signalEvent1Triggered = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/onTrigger[StringEvent]@" + time1, signalEvent1Triggered, zk);

        TestUtils.injectIntoStringSocketAdapter(time1);

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));

        // return latch on trigger signal
        return signalEvent1Triggered;
    }

}
