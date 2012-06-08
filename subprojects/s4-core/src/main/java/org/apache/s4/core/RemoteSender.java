package org.apache.s4.core;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;

public class RemoteSender {

    final private Emitter emitter;
    final private Hasher hasher;
    int targetPartition = 0;

    public RemoteSender(Emitter emitter, Hasher hasher) {
        super();
        this.emitter = emitter;
        this.hasher = hasher;
    }

    public void send(String hashKey, EventMessage eventMessage) {
        if (hashKey == null) {
            // round robin by default
            emitter.send(Math.abs(targetPartition++ % emitter.getPartitionCount()), eventMessage);
        } else {
            int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());
            emitter.send(partition, eventMessage);
        }
    }
}
