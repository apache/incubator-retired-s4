package org.apache.s4.core;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;

public class RemoteSender {

    final private Emitter emitter;
    final private Hasher hasher;

    public RemoteSender(Emitter emitter, Hasher hasher) {
        super();
        this.emitter = emitter;
        this.hasher = hasher;
    }

    public void send(String hashKey, EventMessage eventMessage) {
        if (hashKey == null) {
            for (int i = 0; i < emitter.getPartitionCount(); i++) {
                emitter.send(i, eventMessage);
            }
        } else {
            int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());
            emitter.send(partition, eventMessage);
        }
    }
}
