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
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
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
    private static final String CLUSTER_NAME = "clusterZ";
    private HttpServer httpServer;
    private static File tmpAppsDir;

    @BeforeClass
    public static void createS4RFiles() throws Exception {
        tmpAppsDir = Files.createTempDir();

        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build.gradle"), "installS4R", new String[] { "appsDir="
                + tmpAppsDir.getAbsolutePath() });

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

        assertDeployment(uri);

    }

    private void assertDeployment(final String uri) throws KeeperException, InterruptedException, IOException {
        CountDownLatch signalAppInitialized = new CountDownLatch(1);
        CountDownLatch signalAppStarted = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppInitialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppStarted,
                CommTestUtils.createZkClient());

        ZNRecord record = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        record.putSimpleField(DistributedDeploymentManager.S4R_URI, uri);
        zkClient.create("/s4/clusters/" + CLUSTER_NAME + "/app/s4App", record, CreateMode.PERSISTENT);

        Assert.assertTrue(signalAppInitialized.await(20, TimeUnit.SECONDS));
        Assert.assertTrue(signalAppStarted.await(20, TimeUnit.SECONDS));

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        CommTestUtils
                .watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, CommTestUtils.createZkClient());

        CoreTestUtils.injectIntoStringSocketAdapter(time1);

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));
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

        assertDeployment("http://localhost:8080/s4/" + s4rToDeploy.getName());

    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException {
        // 0. package s4 app
        // TODO this is currently done offline, and the app contains the TestApp class copied from the one in the
        // current package .

        // 1. start s4 nodes. Check that no app is deployed.
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean(CLUSTER_NAME);
        taskSetup.setup(CLUSTER_NAME, 1, 1300);

        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        List<String> processes = zkClient.getChildren("/s4/clusters/" + CLUSTER_NAME + "/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/" + CLUSTER_NAME + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 2) {
                    signalProcessesReady.countDown();
                }

            }
        });

        forkedNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=" + CLUSTER_NAME });

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
