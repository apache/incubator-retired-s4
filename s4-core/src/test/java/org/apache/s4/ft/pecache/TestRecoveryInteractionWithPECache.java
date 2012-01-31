package org.apache.s4.ft.pecache;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.s4.ft.EventGenerator;
import org.apache.s4.ft.KeyValue;
import org.apache.s4.ft.S4App;
import org.apache.s4.ft.S4TestCase;
import org.apache.s4.ft.TestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRecoveryInteractionWithPECache extends S4TestCase {
	
	private static Factory zookeeperServerConnectionFactory;
	private Process forkedS4App;
	
	@Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();

    }
	
	@After
    public void cleanup() throws Exception {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        forkedS4App.destroy();
    }
	
	@Test
	public void testNoRecoveryAfterExpiration() throws Exception {
		byte[] data = testAndReturnValueAfterExpiration("app_conf_noRecoveryAfterExpiration.xml");
		Assert.assertTrue("got: " + new String(data) , new String(data).equals("value2-"));
	}

	@Test
	public void testRecoveryAfterExpiration() throws Exception {
		byte[] data = testAndReturnValueAfterExpiration("app_conf_recoveryAfterExpiration.xml");
		Assert.assertTrue(new String(data), new String(data).equals("value1-value2-"));
	}
	
	
	private byte[] testAndReturnValueAfterExpiration(String appConfig) throws Exception,
			IOException, KeeperException, InterruptedException, JSONException {
		
		forkedS4App = TestUtils.forkS4App(getClass().getName(), "s4_core_conf_fs_backend.xml", appConfig);
//		app.initializeS4App();
		final ZooKeeper zk = TestUtils.createZkClient();
		
		CountDownLatch latch1=new CountDownLatch(1);
		TestUtils.watchAndSignalCreation("/value", latch1,
                zk);
		
		EventGenerator generator = new EventGenerator();
		generator.injectValueEvent(new KeyValue("key", "value1-"),
                    "Values", 0);
		
		latch1.await(10, TimeUnit.SECONDS);
		
		byte[] data = zk.getData("/value", false, null);
		Assert.assertTrue(new String(data).equals("value1-"));
		
		zk.delete("/value", -1);
		
		boolean nodeDeleted = false;
		try {
			zk.getData("/value", false, null);
		} catch (NoNodeException e) {
			nodeDeleted = true;
		}
		Assert.assertTrue(nodeDeleted);
		// NOTE: we need sleeps to wait for checkpoints (in the absence of ZK notifications for that)
		Thread.sleep(1000);
		forkedS4App.destroy();
		forkedS4App = TestUtils.forkS4App(getClass().getName(), "s4_core_conf_fs_backend.xml", appConfig);
		CountDownLatch latch2=new CountDownLatch(1);
		TestUtils.watchAndSignalCreation("/value", latch2,
                zk);
		generator.injectValueEvent(new KeyValue("key", "value2-"),
                "Values", 0);
		latch2.await(10, TimeUnit.SECONDS);
		data = zk.getData("/value", false, null);
		return data;
	}
	
	
	
	

}
