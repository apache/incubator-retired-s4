package org.apache.s4.deploy;

import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.LoggerFactory;

public class SimplePE extends ProcessingElement implements Watcher {

    private ZooKeeper zk;

    public SimplePE() {
    }

    public SimplePE(App app) {
        super(app);
    }

    public void onEvent(org.apache.s4.base.Event event) {
        try {
            LoggerFactory.getLogger(getClass()).debug("processing envent {}", event.get("line"));
            zk.create("/onEvent@" + event.get("line"), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void onCreate() {
        if (zk == null) {
            try {
                zk = new ZooKeeper("localhost:" + 21810, 4000, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }
}
