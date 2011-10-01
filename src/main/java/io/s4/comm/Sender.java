package io.s4.comm;

import com.google.inject.Inject;

import io.s4.core.Event;
import io.s4.core.Hasher;
import io.s4.serialize.SerializerDeserializer;

/**
 * The {@link Sender} and its counterpart {@link Receiver} are the top level
 * classes of the communication layer. 
 * <p>
 * {@link Sender} is responsible for sending an event to a
 * {@link ProcessingElement} instance using a hashKey.
 * <p>
 * Details on how the cluster is partitioned and how events are serialized and
 * transmitted to its destination are hidden from the application developer.
 */
public class Sender {
    final private Emitter emitter;
    final private SerializerDeserializer serDeser;
    final private Hasher hasher;

    /**
     * 
     * @param emitter
     *            the emitter implements the low level commiunication layer.
     * @param serDeser
     *            a serialization mechanism.
     * @param hasher
     *            a hashing function to map keys to partition IDs.
     */
    @Inject
    public Sender(Emitter emitter, SerializerDeserializer serDeser,
            Hasher hasher) {
        this.emitter = emitter;
        this.serDeser = serDeser;
        this.hasher = hasher;
    }

    /**
     * @param hashKey
     *            the string used to map the value of a key to a specific
     *            partition.
     * @param event
     *            the event to be delivered to a {@link ProcessingElement}
     *            instance.
     */
    public void send(String hashKey, Event event) {
        // System.out.println("SenderNonParam: Sending event " + event);
        int partition = (int) (hasher.hash(hashKey) % emitter
                .getPartitionCount());
        send(partition, event);
    }

    private void send(int partition, Event event) {
        // serialize and send
        byte[] blob = serDeser.serialize(event);
        emitter.send(partition, blob);
    }

    /**
     * Send an event to all the partitions in the cluster.
     * 
     * @param event
     *            the event to be delivered to {@link ProcessingElement}
     *            instances.
     */
    public void send(Event event) {
        byte[] blob = serDeser.serialize(event);
        for (int i = 0; i < emitter.getPartitionCount(); i++) {
            emitter.send(i, blob);
        }
    }
}
