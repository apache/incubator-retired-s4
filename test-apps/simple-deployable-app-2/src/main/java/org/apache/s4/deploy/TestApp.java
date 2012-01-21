package org.apache.s4.deploy;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.core.App;
import org.apache.zookeeper.CreateMode;

public class TestApp extends App {

    private ZkClient zkClient;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {
            zkClient = new ZkClient("localhost:" + 21810);
            if (!zkClient.exists("/s4-test")) {
                zkClient.create("/s4-test", null, CreateMode.PERSISTENT);
            }
            zkClient.createEphemeral(AppConstants.INITIALIZED_ZNODE_2, null);
        } catch (Exception e) {
            System.exit(-1);
        }
    }

    @Override
    protected void onStart() {
        try {
            Class.forName("org.apache.s4.deploy.A").getConstructor(ZkClient.class).newInstance(zkClient);
        } catch (Exception e) {
            System.exit(-1);
        }
    }
}
