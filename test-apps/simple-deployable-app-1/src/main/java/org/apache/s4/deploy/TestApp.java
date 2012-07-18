package org.apache.s4.deploy;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.zookeeper.CreateMode;

import com.google.common.collect.ImmutableList;

public class TestApp extends App {

    private ZkClient zkClient;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {
            SimplePE prototype = createPE(SimplePE.class);
            Stream<Event> stream = createInputStream("inputStream", new KeyFinder<Event>() {
                public java.util.List<String> get(Event event) {
                    return ImmutableList.of("line");
                }
            }, prototype);
            zkClient = new ZkClient("localhost:" + 2181);
            if (!zkClient.exists("/s4-test")) {
                zkClient.create("/s4-test", null, CreateMode.PERSISTENT);
            }
            zkClient.createEphemeral(AppConstants.INITIALIZED_ZNODE_1, null);
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
