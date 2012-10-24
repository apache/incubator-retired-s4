package org.apache.s4.tools.yarn;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.AppConstants;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.apache.s4.tools.Tools;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.inject.Injector;

public class TestYarnDeployment extends ZkBasedTest {

    private static Logger logger = LoggerFactory.getLogger(TestYarnDeployment.class);

    protected static MiniYARNCluster yarnCluster = null;
    protected static MiniDFSCluster dfsCluster = null;
    protected static Configuration conf = new Configuration();

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        logger.info("Starting up YARN cluster");
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);

        // NOTE : we use default ports
        dfsCluster = new MiniDFSCluster.Builder(conf).nameNodePort(9000).build();
        dfsCluster.waitActive();

        String fsDefaultName = "hdfs://localhost:" + dfsCluster.getNameNodePort();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fsDefaultName);

        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);

        yarnCluster = new MiniYARNCluster(TestYarnDeployment.class.getName(), 1, 1, 1);
        yarnCluster.init(conf);
        yarnCluster.start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.info("setup thread sleep interrupted. message=" + e.getMessage());
        }

    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (yarnCluster != null) {
            yarnCluster.stop();
            yarnCluster = null;
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    private ZkClient zkClient;

    private File tmpAppsDir;

    @Test
    public void testDeployment() throws Exception {

        tmpAppsDir = Files.createTempDir();

        File gradlewFile = CoreTestUtils.findGradlewInRootDir();

        CoreTestUtils.callGradleTask(new File(gradlewFile.getParentFile().getAbsolutePath()
                + "/test-apps/simple-deployable-app-1/build.gradle"), "installS4R", new String[] { "appsDir="
                + tmpAppsDir.getAbsolutePath() });

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

        CountDownLatch signalAppInitialized = new CountDownLatch(1);
        CountDownLatch signalAppStarted = new CountDownLatch(1);
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppInitialized,
                CommTestUtils.createZkClient());
        CommTestUtils.watchAndSignalCreation(AppConstants.INITIALIZED_ZNODE_1, signalAppStarted,
                CommTestUtils.createZkClient());

        Assert.assertFalse(zkClient.exists(AppConstants.INITIALIZED_ZNODE_1));

        FileSystem fs = FileSystem.get(conf);
        Path destS4rPath = new Path(fs.getHomeDirectory() + "/simpleDeployableApp.s4r");
        fs.copyFromLocalFile(new Path(new File(tmpAppsDir.getAbsolutePath()
                + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r").toURI()), destS4rPath);

        fs.close();

        // File s4rToDeploy = File.createTempFile("testapp" + System.currentTimeMillis(), "s4r");
        //
        // Assert.assertTrue(ByteStreams.copy(
        // Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath()
        // + "/simple-deployable-app-1-0.0.0-SNAPSHOT.s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

        // deploy with Yarn Client
        final String[] params = ("-cluster=cluster1 -nbTasks=2 -flp=14000 -s4r=" + destS4rPath.toUri().toString()
                + " -zk=localhost:2181 -s4Dir=" + gradlewFile.getParentFile().getAbsolutePath()).split("[ ]");

        YarnArgs yarnArgs = new YarnArgs();

        Tools.parseArgs(yarnArgs, params);
        final S4YarnClient client = new S4YarnClient(yarnArgs, conf);

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {

                try {

                    boolean result = client.run(true);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }

            }
        }, "Yarn Client");
        t.start();

        Assert.assertTrue(signalAppInitialized.await(200, TimeUnit.SECONDS));
        Assert.assertTrue(signalAppStarted.await(200, TimeUnit.SECONDS));

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

        // cleanup
        client.killApplication(client.getApplicationList().get(0).getApplicationId());
        // LOG.info("Initializing DS Client");
        // Client client = new Client();
        // boolean initSuccess = client.init(args);
        // assert (initSuccess);
        // LOG.info("Running DS Client");
        // boolean result = client.run();
        //
        // LOG.info("Client run completed. Result=" + result);
        // assert (result == true);

    }
}
