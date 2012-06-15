package org.apache.s4.core;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;

/**
 * The {@link Sender} and its counterpart {@link Receiver} are the top level classes of the communication layer.
 * <p>
 * {@link Sender} is responsible for sending an event to a {@link ProcessingElement} instance using a hashKey.
 * <p>
 * Details on how the cluster is partitioned and how events are serialized and transmitted to its destination are hidden
 * from the application developer.
 */
public class Sender {
    final private Emitter emitter;
    final private SerializerDeserializer serDeser;
    final private Hasher hasher;

    /*
     * If the local partition id is not initialized, always use the comm layer to send events.
     */
    private int localPartitionId = -1;

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
    public Sender(Emitter emitter, SerializerDeserializer serDeser, Hasher hasher) {
        this.emitter = emitter;
        this.serDeser = serDeser;
        this.hasher = hasher;
    }

    /**
     * This method attempts to send an event to a remote partition. If the destination is local, the method does not
     * send the event and returns true. The caller is expected to put the event in a local queue instead.
     * 
     * @param hashKey
     *            the string used to map the value of a key to a specific partition.
     * @param event
     *            the event to be delivered to a {@link ProcessingElement} instance.
     * @return true if the event is not sent because the destination is local.
     * 
     */
    public boolean sendAndCheckIfLocal(String hashKey, Event event) {
        int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());

        if (partition == localPartitionId) {
            /* Hey we are in the same JVM, don't use the network. */
            return true;
        }
        send(partition,
                new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), serDeser.serialize(event)));
        return false;
    }

    private void send(int partition, EventMessage event) {
        emitter.send(partition, event);
    }

    /**
     * Send an event to all the remote partitions in the cluster. The caller is expected to also put the event in a
     * local queue.
     * 
     * @param event
     *            the event to be delivered to {@link ProcessingElement} instances.
     */
    public void sendToRemotePartitions(Event event) {

        for (int i = 0; i < emitter.getPartitionCount(); i++) {

            /* Don't use the comm layer when we send to the same partition. */
            if (localPartitionId != i)
                emitter.send(
                        i,
                        new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), serDeser
                                .serialize(event)));
        }
    }

    void setPartition(int partitionId) {
        localPartitionId = partitionId;
    }
}
