package org.apache.s4.base;

import java.nio.ByteBuffer;

/**
 * Defines the entry point from the communication layer to the application layer.
 * 
 * Events received as raw bytes through the {@link Listener} implementation use the {@link Receiver#receive(ByteBuffer)}
 * method so that events can be deserialized (conversion from byte[] to Event objects) and enqueued for processing.
 * 
 */
public interface Receiver {

    /**
     * Handle a serialized message, i.e. deserialize the message and pass it to event processors.
     */
    void receive(ByteBuffer message);

    /**
     * Returns the partition id currently assigned to this node.
     */
    int getPartitionId();

}
