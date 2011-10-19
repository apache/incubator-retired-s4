package test.s4.core.triggers;


import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import test.s4.fixtures.TestUtils;
import test.s4.wordcount.StringEvent;

public class TriggerablePE extends ProcessingElement implements Watcher {
    
    private ZooKeeper zk;

    public TriggerablePE() {}
    
    public TriggerablePE(App app) {
        super(app);
    }
    
    public void onEvent(StringEvent event) {
        try {
            zk.create("/onEvent@"+event.getString(), new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
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
                zk = new ZooKeeper("localhost:" + TestUtils.ZK_PORT, 4000, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void onTrigger(StringEvent event) {
        try {
            zk.create("/onTrigger[StringEvent]@"+event.getString(), new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
