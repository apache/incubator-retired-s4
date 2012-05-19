package org.apache.s4.deploy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Tests deployment of packaged applications <br>
 * - loaded from local apps directory <br>
 * - deployed through zookeeper notification <br>
 * - ... from the file system <br>
 * - ... or from a web server
 * 
 */
public class TestAutomaticDeployment {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedNode;
    private ZkClient zkClient;
    private String clusterName;
    private HttpServer httpServer;
    private static File tmpAppsDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        tmpAppsDir = Files.createTempDir();

        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build.gradle"), "installS4R", new String[] { "appsDir="
                + tmpAppsDir.getAbsolutePath() });

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-2/build.gradle"), "installS4R", new String[] { "appsDir="
                + tmpAppsDir.getAbsolutePath() });
    }

    @Before
    public void cleanLocalAppsDir() throws ConfigurationException, IOException {
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

    private PropertiesConfiguration loadConfig() throws ConfigurationException, IOException {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(Resources.newInputStreamSupplier(Resources.getResource("default.s4.properties")).getInput());
        return config;
    }

    // ignore this test since now we only deploy from artifacts published through zookeeper
    @Test
    @Ignore
    public void testInitialDeploymentFromFileSystem() throws Exception {

        File s4rToDeploy = new File(loadConfig().getString("appsDir") + File.separator + "testapp"
                + System.currentTimeMillis() + ".s4r");

        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                        + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        initializeS4Node();

        final String uri = s4rToDeploy.toURI().toString();

        assertDeployment(uri, true);

    }

    @Test
    public void testZkTriggeredDeploymentFromFileSystem() throws Exception {

        initializeS4Node();

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        File s4rToDeploy = File.createTempFile("testapp" + System.currentTimeMillis(), "s4r");

        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                        + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        final String uri = s4rToDeploy.toURI().toString();

        assertDeployment(uri, false);

    }

    private void assertDeployment(final String uri, boolean initial) throws KeeperException, InterruptedException,
            IOException {
        CountDownLatch signalAppInitialized = new CountDownLatch(1);
        CountDownLatch signalAppStarted = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppInitialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppStarted,
                CommTestUtils.createZkClient());

        if (!initial) {
            ZNRecord record = new ZNRecord(String.valueOf(System.currentTimeMillis()));
            record.putSimpleField(DistributedDeploymentManager.S4R_URI, uri);
            zkClient.create("/s4/clusters/" + clusterName + "/apps/testApp", record, CreateMode.PERSISTENT);
        }

        Assert.assertTrue(signalAppInitialized.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(signalAppStarted.await(10, TimeUnit.SECONDS));

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        CommTestUtils
                .watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, CommTestUtils.createZkClient());

        CoreTestUtils.injectIntoStringSocketAdapter(time1);

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));
    }

    private void assertMultipleAppsDeployment(String uri1, String uri2) throws KeeperException, InterruptedException,
            IOException {
        CountDownLatch signalApp1Initialized = new CountDownLatch(1);
        CountDownLatch signalApp1Started = new CountDownLatch(1);

        CountDownLatch signalApp2Initialized = new CountDownLatch(1);
        CountDownLatch signalApp2Started = new CountDownLatch(1);

        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalApp1Initialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_2, signalApp1Started,
                CommTestUtils.createZkClient());

        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_2, signalApp2Initialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.STARTED_ZNODE_2, signalApp2Started,
                CommTestUtils.createZkClient());

        ZNRecord record1 = new ZNRecord(String.valueOf(System.currentTimeMillis()) + "-app1");
        record1.putSimpleField(DistributedDeploymentManager.S4R_URI, uri1);
        zkClient.create("/s4/clusters/" + clusterName + "/apps/testApp1", record1, CreateMode.PERSISTENT);

        ZNRecord record2 = new ZNRecord(String.valueOf(System.currentTimeMillis()) + "-app2");
        record2.putSimpleField(DistributedDeploymentManager.S4R_URI, uri2);
        zkClient.create("/s4/clusters/" + clusterName + "/apps/testApp2", record2, CreateMode.PERSISTENT);

        Assert.assertTrue(signalApp1Initialized.await(20, TimeUnit.SECONDS));
        Assert.assertTrue(signalApp1Started.await(10, TimeUnit.SECONDS));

        Assert.assertTrue(signalApp2Initialized.await(20, TimeUnit.SECONDS));
        Assert.assertTrue(signalApp2Started.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testZkTriggeredDeploymentFromHttp() throws Exception {
        initializeS4Node();

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        File tmpDir = Files.createTempDir();

        File s4rToDeploy = new File(tmpDir, String.valueOf(System.currentTimeMillis()));

        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                        + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        // we start a
        InetSocketAddress addr = new InetSocketAddress(8080);
        httpServer = HttpServer.create(addr, 0);

        httpServer.createContext("/s4", new MyHandler(tmpDir));
        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();

        assertDeployment("http://localhost:8080/s4/" + s4rToDeploy.getName(), false);

    }

    /**
     * * * Tests that classes with same signature are loaded in different class loaders (through the S4RLoader), even
     * when referenced through reflection, and even when referencing classes present in the classpath of the S4 nod * *
     * Works in the following manne * * - we have app1 and app2, very simple a * * - app1 and app2 have 3 classes with
     * same name: A, AppConstants and Tes * * - app1 in addition has a PE and a socket adapter so that it can react to
     * injected e * * - upon initialization of the application, TestApp writes a znode in Zookeeper, corresponding to
     * the application index (1 or 2), using the corresponding constant from the AppConstant class (which is part of the
     * S4 node classpath, and therefore loaded by the standard classloader, not from an s4 app classl *
     * 
     * - upon startup of the application, TestApp creates A by reflection, and A writes a znode specific to the current
     * p
     * 
     * - app1 and app2 are generated through gradle scripts, called by executing the "gradlew" executable at the root of
     * the project, and using the build.gradle file available for these appl * ns
     * 
     * - app1 and app2 s4r archives are copied to a web server and published to * per
     * 
     * - they automatically get deployed, and we verify that 2 apps are correctly started, therefore that classes
     * TestApp and A were independently loaded for independent ap * ions
     * 
     */

    @Test
    public void testZkTriggeredDeploymentFromHttpForMultipleApps() throws Exception {
        initializeS4Node();
        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));
        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_2));

        File tmpDir = Files.createTempDir();

        File s4rToDeployForApp1 = new File(tmpDir, String.valueOf(System.currentTimeMillis()) + "-app1");
        File s4rToDeployForApp2 = new File(tmpDir, String.valueOf(System.currentTimeMillis()) + "-app2");

        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                        + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r")),
                Files.newOutputStreamSupplier(s4rToDeployForApp1)) > 0);
        Assert.assertTrue(ByteStreams.copy(
                Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
                        + "/simple-deployable-app-2-0.0.0-SNAPSHOT.s4r")),
                Files.newOutputStreamSupplier(s4rToDeployForApp2)) > 0);

        // we start a
        InetSocketAddress addr = new InetSocketAddress(8080);
        httpServer = HttpServer.create(addr, 0);

        httpServer.createContext("/s4", new MyHandler(tmpDir));
        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();

        assertMultipleAppsDeployment("http://localhost:8080/s4/" + s4rToDeployForApp1.getName(),
                "http://localhost:8080/s4/" + s4rToDeployForApp2.getName());

    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException {
        // 0. package s4 app
        // TODO this is currently done offline, and the app contains the TestApp class copied from the one in the
        // current package .

        // 1. start s4 nodes. Check that no app is deployed.
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(Resources.newInputStreamSupplier(Resources.getResource("default.s4.properties")).getInput());

        clusterName = config.getString("cluster.name");
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean(clusterName);
        taskSetup.setup(clusterName, 1, 1300);

        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        List<String> processes = zkClient.getChildren("/s4/clusters/" + clusterName + "/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/" + clusterName + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 2) {
                    signalProcessesReady.countDown();
                }

            }
        });

        File tmpConfig = File.createTempFile("tmp", "config");
        Assert.assertTrue(ByteStreams.copy(getClass().getResourceAsStream("/default.s4.properties"),
                Files.newOutputStreamSupplier(tmpConfig)) > 0);
        forkedNode = CoreTestUtils.forkS4Node(new String[] { tmpConfig.getAbsolutePath() });

        // TODO synchro with ready state from zk
        Thread.sleep(10000);
        // Assert.assertTrue(signalProcessesReady.await(10, TimeUnit.SECONDS));

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
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    public static void main(String[] args) throws IOException {

        System.out.println("Server is listening on port 8080");
    }
}

class MyHandler implements HttpHandler {

    File tmpDir;

    public MyHandler(File tmpDir) {
        this.tmpDir = tmpDir;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (requestMethod.equalsIgnoreCase("GET")) {
            String fileName = exchange.getRequestURI().getPath().substring("/s4/".length());
            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.set(HttpHeaders.Names.CONTENT_TYPE, HttpHeaders.Values.BYTES);
            exchange.sendResponseHeaders(200, Files.toByteArray(new File(tmpDir, fileName)).length);

            OutputStream responseBody = exchange.getResponseBody();

            ByteStreams.copy(new FileInputStream(new File(tmpDir, fileName)), responseBody);

            responseBody.close();
        }
    }
}
