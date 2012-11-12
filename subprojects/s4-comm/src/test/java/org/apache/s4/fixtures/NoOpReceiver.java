package org.apache.s4.fixtures;

import java.nio.ByteBuffer;

import org.apache.s4.base.Receiver;

/**
 * Avoids delegating message processing to the application layer.
 * 
 */
class NoOpReceiver implements Receiver {

    @Override
    public void receive(ByteBuffer message) {
        // do nothing
    }

    @Override
    public int getPartitionId() {
        throw new RuntimeException("Not implemented");
    }
}
