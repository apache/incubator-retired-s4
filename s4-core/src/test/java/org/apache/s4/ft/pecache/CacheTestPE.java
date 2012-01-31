package org.apache.s4.ft.pecache;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.s4.ft.KeyValue;
import org.apache.s4.ft.TestUtils;
import org.apache.s4.processor.AbstractPE;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class CacheTestPE extends AbstractPE implements Watcher {

	String value = "";
	transient ZooKeeper zk = null;

	public void processEvent(KeyValue event) {
		if (zk == null) {
			Logger.getLogger(getClass()).info("Creating ZK connection");
            try {
                zk = new ZooKeeper("localhost:" + TestUtils.ZK_PORT, 4000, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
		if ("key".equals(event.getKey())) {
			setValue(this.value + event.getValue());
			try {
				Logger.getLogger(getClass()).info("setting ZK /value");
				zk.create("/value", value.getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new RuntimeException("unknown event " + event);

		}
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}

}
