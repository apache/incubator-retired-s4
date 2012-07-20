package org.apache.s4.core.ft;

import org.apache.s4.core.ProcessingElement;

/**
 * Implementation of {@link CheckpointingFramework} that does NO checkpointing.
 *
 */
public final class NoOpCheckpointingFramework implements CheckpointingFramework {

    @Override
    public StorageCallback saveState(ProcessingElement pe) {
        return null;
    }

    @Override
    public byte[] fetchSerializedState(CheckpointId key) {
        return null;
    }

    @Override
    public boolean isCheckpointable(ProcessingElement pe) {
        return false;
    }

}
