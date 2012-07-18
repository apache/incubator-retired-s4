package org.apache.s4.core.ft;

import com.google.inject.AbstractModule;

/**
 * Checkpointing module that uses the {@link DefaultFileSystemStateStorage} as a checkpointing backend.
 *
 */
public class FileSystemBackendCheckpointingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(StateStorage.class).to(DefaultFileSystemStateStorage.class);
        bind(CheckpointingFramework.class).to(SafeKeeper.class);
    }
}
