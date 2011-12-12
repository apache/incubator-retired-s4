package org.apache.s4.core.apploading;

import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.fixtures.SocketAdapter;
import org.apache.s4.fixtures.TestUtils;
import org.apache.s4.wordcount.SentenceKeyFinder;
import org.apache.s4.wordcount.StringEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class SimpleApp extends App {
    private SocketAdapter<StringEvent> socketAdapter;

    public SimpleApp() {
    }

    @Override
    protected void onStart() {
        try {
            final ZooKeeper zk = TestUtils.createZkClient();
            zk.create("/simpleAppCreated", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
        } catch (Exception e) {
            System.exit(-1);
        }
    }

    @Override
    protected void onInit() {
        SimplePE prototype = createPE(SimplePE.class);
        Stream<StringEvent> stream = createStream("stream", new SentenceKeyFinder(), prototype);
        try {
            socketAdapter = new SocketAdapter<StringEvent>(stream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub
    }
}
