package org.apache.s4.deploy.prodcon;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DistributedDeploymentManager;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.sun.net.httpserver.HttpServer;

public class TestProducerConsumer {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedNode;
    private ZkClient zkClient;
    private String clusterName;
    private HttpServer httpServer;
    private static File tmpAppsDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        tmpAppsDir = Files.createTempDir();

        Assert.assertTrue(tmpAppsDir.exists());
        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(gradlewFile, new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/s4-showtime/build.gradle"), "installS4R",
                new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });

        CoreTestUtils.callGradleTask(gradlewFile, new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/s4-counter/build.gradle"), "installS4R",
                new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });
    }

    @Before
    public void cleanLocalAppsDir() throws ConfigurationException {
        PropertiesConfiguration config = loadConfig();

        if (!new File(config.getString("appsDir")).exists()) {
            Assert.assertTrue(new File(config.getString("appsDir")).mkdirs());
        } else {
            if (!config.getString("appsDir").startsWith("/tmp")) {
                Assert.fail("apps dir should a subdir of /tmp for safety");
            }
            CommTestUtils.deleteDirectoryContents(new File(config.getString("appsDir")));
        }
    }

    @Before
    public void prepare() throws Exception {
        CommTestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = CommTestUtils.startZookeeperServer();
        final ZooKeeper zk = CommTestUtils.createZkClient();
        try {
            zk.delete("/simpleAppCreated", -1);
        } catch (Exception ignored) {
        }

        zk.close();
    }

    @After
    public void cleanup() throws Exception {
        CommTestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        CommTestUtils.killS4App(forkedNode);
    }

    private PropertiesConfiguration loadConfig() throws org.apache.commons.configuration.ConfigurationException {
        InputStream is = this.getClass().getResourceAsStream("/org.apache.s4.deploy.s4.properties");
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(is);
        return config;
    }

    @Test
    public void testInitialDeploymentFromFileSystem() throws Exception {

        File showtimeS4R = new File(loadConfig().getString("appsDir") + File.separator + "showtime"
                + System.currentTimeMillis() + ".s4r");
        System.out.println(tmpAppsDir.getAbsolutePath());
        Assert.assertTrue(ByteStreams.copy(Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                + "/s4-showtime-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(showtimeS4R)) > 0);
        String uriShowtime = showtimeS4R.toURI().toString();

        File counterS4R = new File(loadConfig().getString("appsDir") + File.separator + "counter"
                + System.currentTimeMillis() + ".s4r");
        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath() + "/s4-counter-0.0.0-SNAPSHOT.s4r")),
                Files.newOutputStreamSupplier(counterS4R)) > 0);

        String uriCounter = counterS4R.toURI().toString();

        initializeS4Node();

        ZNRecord record1 = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        record1.putSimpleField(DistributedDeploymentManager.S4R_URI, uriShowtime);
        zkClient.create("/" + clusterName + "/apps/showtime", record1, CreateMode.PERSISTENT);

        ZNRecord record2 = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        record2.putSimpleField(DistributedDeploymentManager.S4R_URI, uriCounter);
        zkClient.create("/" + clusterName + "/apps/counter", record2, CreateMode.PERSISTENT);

        // TODO validate test through some Zookeeper notifications
        Thread.sleep(10000);
    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException {
        // 0. package s4 app
        // TODO this is currently done offline, and the app contains the TestApp class copied from the one in the
        // current package .

        // 1. start s4 nodes. Check that no app is deployed.
        InputStream is = this.getClass().getResourceAsStream("/org.apache.s4.deploy.s4.properties");
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(is);

        clusterName = config.getString("cluster.name");
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 1, 1300);

        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        List<String> processes = zkClient.getChildren("/" + clusterName + "/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/" + clusterName + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 2) {
                    signalProcessesReady.countDown();
                }

            }
        });

        File tmpConfig = File.createTempFile("tmp", "config");
        Assert.assertTrue(ByteStreams.copy(getClass().getResourceAsStream("/org.apache.s4.deploy.s4.properties"),
                Files.newOutputStreamSupplier(tmpConfig)) > 0);
        forkedNode = CoreTestUtils.forkS4Node(new String[] { tmpConfig.getAbsolutePath() });

        // TODO synchro with ready state from zk
        Thread.sleep(10000);
        // Assert.assertTrue(signalProcessesReady.await(10, TimeUnit.SECONDS));

    }

}
