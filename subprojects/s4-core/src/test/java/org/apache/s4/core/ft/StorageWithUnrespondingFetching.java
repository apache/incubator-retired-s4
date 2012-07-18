package org.apache.s4.core.ft;

import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class StorageWithUnrespondingFetching implements StateStorage {

    @Inject
    DefaultFileSystemStateStorage storage = new DefaultFileSystemStateStorage();

    @Inject
    @Named("s4.checkpointing.filesystem.storageRootPath")
    String storageRootPath;

    @Inject
    private void initStorageRootPath() {
        // manual init because we are not directly injecting the default file system storage, but creating a new
        // instance in this class
        storage.storageRootPath = this.storageRootPath;
    }

    @Override
    public void saveState(CheckpointId key, byte[] state, StorageCallback callback) {
        storage.saveState(key, state, callback);
    }

    @Override
    public byte[] fetchState(CheckpointId key) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return storage.fetchState(key);
    }

    @Override
    public Set<CheckpointId> fetchStoredKeys() {
        return storage.fetchStoredKeys();
    }

}
