package org.apache.s4.wordcount;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsolationWordCounterPE extends ProcessingElement implements Watcher {
    private static Logger logger = LoggerFactory.getLogger(IsolationWordCounterPE.class);

    transient private ZooKeeper zk;
    private String zkPath;
    static int count = 0;
    int wordCounter;
    transient Stream<WordCountEvent> wordClassifierStream;
    public static AtomicInteger prototypeId = new AtomicInteger();

    private IsolationWordCounterPE() {

    }

    public IsolationWordCounterPE(App app) {
        super(app);
        if (zk == null) {
            try {
                zk = new ZooKeeper("localhost:2181", 4000, this);
                synchronized (prototypeId) {
                    zkPath = "/counters/counter_prototype_" + prototypeId.incrementAndGet() + "_"
                            + System.currentTimeMillis();
                    zk.create(zkPath, "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void onEvent(WordSeenEvent event) {

        wordCounter++;
        System.out.println("seen word " + event.getWord());

        // NOTE: it seems the id is the key for now...
        wordClassifierStream.put(new WordCountEvent(getId(), wordCounter));
        // Update the zookeeper
        synchronized (this) {
            count++;
            try {
                zk.setData(zkPath, String.valueOf(count).getBytes(), -1);
                logger.info("set " + zkPath + " " + count);
            } catch (KeeperException e) {
                logger.error(zkPath + " " + count, e);
            } catch (InterruptedException e) {
                logger.error(zkPath + " " + count, e);
            }
        }
    }

    public void setWordClassifierStream(Stream<WordCountEvent> stream) {
        this.wordClassifierStream = stream;
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
