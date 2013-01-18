package org.apache.s4.fixtures;

import java.nio.ByteBuffer;

import org.apache.s4.base.Receiver;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;

/**
 * For tests purposes, intercepts messages that would normally be delegated to the application layer.
 * 
 */
public class MockReceiver implements Receiver {

    SerializerDeserializer serDeser;

    @Inject
    public MockReceiver(SerializerDeserializer serDeser) {
        super();
        this.serDeser = serDeser;
    }

    @Override
    public void receive(ByteBuffer message) {
        if (CommTestUtils.MESSAGE.equals(serDeser.deserialize(message))) {
            CommTestUtils.SIGNAL_MESSAGE_RECEIVED.countDown();
        } else {
            System.err.println("Unexpected message:" + serDeser.deserialize(message));
        }

    }

    @Override
    public int getPartitionId() {
        throw new RuntimeException("Not implemented");
    }
}
