package test.s4.core.apploading;

import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import test.s4.fixtures.SocketAdapter;
import test.s4.fixtures.TestUtils;
import test.s4.wordcount.SentenceKeyFinder;
import test.s4.wordcount.StringEvent;

public class SimpleApp extends App {
    private SocketAdapter<StringEvent> socketAdapter;
    
    public SimpleApp () {}

    @Override
    protected void start() {
        try {
            final ZooKeeper zk = TestUtils.createZkClient();
            zk.create("/simpleAppCreated", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
        } catch (Exception e) {
            System.exit(-1);
        }
    }

    @Override
    protected void init() {
        SimplePE prototype = createPE(SimplePE.class);
        Stream<StringEvent> stream = createStream("stream", new SentenceKeyFinder(), prototype);
        try {
            socketAdapter = new SocketAdapter<StringEvent>(stream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void close() {
        // TODO Auto-generated method stub
    }
}