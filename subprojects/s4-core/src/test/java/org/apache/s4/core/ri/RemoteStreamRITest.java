package org.apache.s4.core.ri;

import static org.apache.s4.core.ri.RuntimeIsolationTest.counterNumber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.wordcount.WordCountModule;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class RemoteStreamRITest extends RuntimeIsolationTest {

    private static Logger logger = LoggerFactory.getLogger(RemoteStreamRITest.class);

    @Override
    public void injectData() throws InterruptedException, IOException {
        // Use remote stream

    }

    @Override
    public void startNodes() throws IOException, InterruptedException {
        final ZooKeeper zk = CommTestUtils.createZkClient();

        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.setup("cluster2", 1, 1500);

        s4nodes = new Process[numberTasks + 1];

        List<Process> nodes = new ArrayList<Process>();

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(IsolationWordCountApp.class.getName())
                .customModulesNames(ImmutableList.of(WordCountModule.class.getName())).build(), "cluster1", false,
                "localhost:2181");
        nodes.addAll(Arrays.asList(CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster1" }, new ZkClient(
                "localhost:2181"), 10, "cluster1", numberTasks)));

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(RemoteAdapterApp.class.getName())
                .customModulesNames(ImmutableList.of(WordCountModule.class.getName())).build(), "cluster2", false,
                "localhost:2181");

        nodes.addAll(Arrays.asList(CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster2" }, new ZkClient(
                "localhost:2181"), 10, "cluster2", 1)));

        s4nodes = nodes.toArray(new Process[] {});
        
        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        try {
            CommTestUtils.watchAndSignalCreation("/results", signalTextProcessed, zk);
            // add authorizations for processing
            for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS + 1; i++) {
                zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }

//            injectData();

            Assert.assertTrue(signalTextProcessed.await(30, TimeUnit.SECONDS));
            String results = new String(zk.getData("/results", false, null));
            Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", results);

            List<String> counterInstances = zk.getChildren("/counters", false);

            int totalCount = 0;
            int activeInstances = 0;
            for (String instance : counterInstances) {
                int count = Integer.parseInt(new String(zk.getData("/counters/" + instance, false, null)));
                if (count != 0) {
                    activeInstances++;
                }
                totalCount += count;
            }
            Assert.assertEquals(numberTasks, counterInstances.size());
            Assert.assertEquals(counterNumber, activeInstances);

            Assert.assertEquals(13, totalCount);
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        
    }

    @Override
    @Test
    public void testSimple() {
        ZooKeeper zk;
        try {
            zk = CommTestUtils.createZkClient();
            zk.create("/counters", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            startNodes();

        } catch (IOException e) {
            logger.error("", e);
        } catch (KeeperException e) {
            logger.error("", e);
        } catch (InterruptedException e) {
            logger.error("", e);
        }

    }
}
