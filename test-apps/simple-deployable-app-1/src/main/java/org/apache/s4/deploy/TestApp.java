package org.apache.s4.deploy;

import java.io.IOException;
import java.util.ArrayList;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.zookeeper.CreateMode;

public class TestApp extends App {

    private ZkClient zkClient;
    private SocketAdapter socketAdapter;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {
            try {
                SimplePE prototype = createPE(SimplePE.class);
                Stream<Event> stream = createStream("stream", new KeyFinder<Event>() {
                    public java.util.List<String> get(Event event) {
                        return new ArrayList<String>() {
                            {
                                add("line");
                            }
                        };
                    }
                }, prototype);
                socketAdapter = new SocketAdapter(stream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
