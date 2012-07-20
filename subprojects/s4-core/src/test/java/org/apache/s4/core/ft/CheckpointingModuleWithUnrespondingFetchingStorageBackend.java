package org.apache.s4.core.ft;

import org.apache.s4.core.ft.FileSystemBasedBackendWithZKStorageCallbackCheckpointingModule.DummyZKStorageCallbackFactory;
import org.apache.s4.fixtures.CommTestUtils;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class CheckpointingModuleWithUnrespondingFetchingStorageBackend extends AbstractModule {

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named("s4.checkpointing.filesystem.storageRootPath")).toInstance(
                CommTestUtils.DEFAULT_STORAGE_DIR.getAbsolutePath());
        bind(StateStorage.class).to(StorageWithUnrespondingFetching.class);
        bind(CheckpointingFramework.class).to(SafeKeeper.class);
        bind(StorageCallbackFactory.class).to(DummyZKStorageCallbackFactory.class);
    }

}
