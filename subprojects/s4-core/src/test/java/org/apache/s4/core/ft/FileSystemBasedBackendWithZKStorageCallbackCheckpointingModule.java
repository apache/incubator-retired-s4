package org.apache.s4.core.ft;

import org.apache.s4.core.ft.CheckpointingFramework.StorageResultCode;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.google.inject.name.Names;

/**
 * Creates the /checkpointed znode if a successful checkpointing callback is received. Does it only once.
 *
 */
public class FileSystemBasedBackendWithZKStorageCallbackCheckpointingModule extends
        FileSystemBackendCheckpointingModule {

    @Override
    protected void configure() {
        super.configure();
        bind(StorageCallbackFactory.class).to(DummyZKStorageCallbackFactory.class);
        bind(String.class).annotatedWith(Names.named("s4.checkpointing.filesystem.storageRootPath")).toInstance(
                CommTestUtils.DEFAULT_STORAGE_DIR.getAbsolutePath());
    }

    public static class DummyZKStorageCallbackFactory implements StorageCallbackFactory {

        @Override
        public StorageCallback createStorageCallback() {
            return new StorageCallback() {

                @Override
                public void storageOperationResult(StorageResultCode resultCode, Object message) {
                    if (resultCode == StorageResultCode.SUCCESS) {
                        try {
                            ZooKeeper zkClient = CoreTestUtils.createZkClient();
                            zkClient.create("/checkpointed", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                };
            };

        }

    }
}
