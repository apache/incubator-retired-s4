package org.apache.s4.ft;

import java.io.IOException;

import org.apache.s4.processor.AbstractPE;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class StatefulTestPE extends AbstractPE implements Watcher {

	public static final String STATEFUL_TEST_PE_DATA_ZNODE = "/statefulTestPE.data";
	String id;
	String value1 = "";
	String value2 = "";
	transient ZooKeeper zk = null;

	// transient public static File DATA_FILE = new File(
	// System.getProperty("user.dir")
	// + File.separator + "tmp" + File.separator + "StatefulTestPE.data");;

	@Override
	public String getId() {
		return id;
	}

	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

	public void processEvent(KeyValue event) {
		if (zk == null) {
			try {
				zk = new ZooKeeper("localhost:" + TestUtils.ZK_PORT, 4000, this);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		if (!S4TestCase.registeredPEs.containsKey(getSafeKeeperId())) {
			S4TestCase.registeredPEs.put(getSafeKeeperId(), this);
		}
		try {

			if ("value1".equals(event.getKey())) {
				setValue1(event.getValue());
				zk.create("/value1Set", new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else if ("value2".equals(event.getKey())) {
				setValue2(event.getValue());
				zk.create("/value2Set", new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else if ("initiateCheckpoint".equals(event.getKey())) {
				checkpoint();
			} else {
				throw new RuntimeException("unidentified event: " + event);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public String getValue1() {
		return value1;
	}

	public void setValue1(String value1) {
		this.value1 = value1;
		persistValues();
	}

	public String getValue2() {
		return value2;
	}

	public void setValue2(String value2) {
		this.value2 = value2;
		persistValues();
	}

	public void setId(String id) {
		this.id = id;
	}

	protected void checkpoint() {
		super.checkpoint();
		try {
			zk.create("/checkpointed", new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	// NOTE: we use a file as a simple way to keep track of changes
	private void persistValues() {
		if (zk == null) {
			try {
				zk = new ZooKeeper("localhost:" + TestUtils.ZK_PORT, 4000, this);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			try {
				zk.delete(STATEFUL_TEST_PE_DATA_ZNODE, -1);
			} catch (KeeperException e) {
				if (e instanceof KeeperException.NoNodeException) {
					// ignore
				} else {
					throw new RuntimeException(e);
				}
			}
			zk.create(STATEFUL_TEST_PE_DATA_ZNODE, new String("value1="
					+ value1 + " ; value2=" + value2).getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

}
