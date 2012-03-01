package org.apache.s4.ft.rectimeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.s4.ft.DefaultFileSystemStateStorage;
import org.apache.s4.ft.SafeKeeperId;
import org.apache.s4.ft.StateStorage;
import org.apache.s4.ft.StorageCallback;
import org.apache.s4.ft.TestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

// triggered by ZK
public class BrokenStorage implements StateStorage {

	DefaultFileSystemStateStorage storage = new DefaultFileSystemStateStorage();

	CountDownLatch signalFetchable = new CountDownLatch(1);

	public BrokenStorage() {
	}

	@Override
	public void saveState(SafeKeeperId key, byte[] state,
			StorageCallback callback) {
		storage.saveState(key, state, callback);
	}

	@Override
	public byte[] fetchState(SafeKeeperId key) {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		throw new RuntimeException("fetching failed");
	}

	@Override
	public Set<SafeKeeperId> fetchStoredKeys() {
		return storage.fetchStoredKeys();
	}


}
