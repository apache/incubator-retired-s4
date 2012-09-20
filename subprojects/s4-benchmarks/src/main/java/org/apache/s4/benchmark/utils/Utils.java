package org.apache.s4.benchmark.utils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static CountDownLatch getReadySignal(String zkString, final String parentPath, final int counts) {
        ZkClient zkClient = new ZkClient(zkString);
        if (zkClient.exists(parentPath)) {
            System.out.println(parentPath + " path exists and will be deleted");
            zkClient.deleteRecursive(parentPath);
        }
        zkClient.createPersistent(parentPath);
        final CountDownLatch signalReady = new CountDownLatch(1);
        zkClient.subscribeChildChanges(parentPath, new IZkChildListener() {

            @Override
            public void handleChildChange(String arg0, List<String> arg1) throws Exception {

                if (parentPath.equals(arg0)) {
                    if (arg1.size() >= counts) {
                        logger.info("Latch reached for {} with {} children", arg0, counts);
                        signalReady.countDown();
                    }
                }
            }
        });
        return signalReady;
    }

}
