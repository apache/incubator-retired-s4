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
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Injector;
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
public class TestAutomaticDeployment extends ZkBasedTest {

    private Factory zookeeperServerConnectionFactory;
    private Process forkedNode;
    private ZkClient zkClient;
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
        zkClient.create("/s4/clusters/cluster1/app/s4App", record, CreateMode.PERSISTENT);

        Assert.assertTrue(signalAppInitialized.await(20, TimeUnit.SECONDS));
        Assert.assertTrue(signalAppStarted.await(20, TimeUnit.SECONDS));

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        CommTestUtils
                .watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, CommTestUtils.createZkClient());

        Injector injector = CoreTestUtils.createInjectorWithNonFailFastZKClients();

        TCPEmitter emitter = injector.getInstance(TCPEmitter.class);

        Event event = new Event();
        event.put("line", String.class, time1);
        emitter.send(0, new EventMessage("-1", "inputStream", injector.getInstance(SerializerDeserializer.class)
                .serialize(event)));

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

        // check resource loading (we use a zkclient without custom serializer)
        ZkClient client2 = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        Assert.assertEquals("Salut!", client2.readData("/resourceData"));

    }

    private void initializeS4Node() throws ConfigurationException, IOException, InterruptedException {
        // 0. package s4 app
        // TODO this is currently done offline, and the app contains the TestApp class copied from the one in the
        // current package .

        // 1. start s4 nodes. Check that no app is deployed.
        zkClient = new ZkClient("localhost:" + CommTestUtils.ZK_PORT);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        List<String> processes = zkClient.getChildren("/s4/clusters/cluster1/process");
        Assert.assertTrue(processes.size() == 0);
        final CountDownLatch signalProcessesReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/cluster1/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 2) {
                    signalProcessesReady.countDown();
                }

            }
        });

        forkedNode = CoreTestUtils.forkS4Node(new String[] { "-cluster=cluster1" });

        // TODO synchro with ready state from zk
        Thread.sleep(10000);
        // Assert.assertTrue(signalProcessesReady.await(10, TimeUnit.SECONDS));

    }

    // @Before
    // public void clean() throws Exception {
    // final ZooKeeper zk = CommTestUtils.createZkClient();
    // try {
    // zk.delete("/simpleAppCreated", -1);
    // } catch (Exception ignored) {
    // }
    //
    // zk.close();
    // }

    @After
    public void cleanup() throws Exception {
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
