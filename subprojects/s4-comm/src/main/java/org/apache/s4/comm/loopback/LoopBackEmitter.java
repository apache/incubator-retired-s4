package org.apache.s4.comm.loopback;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;

public class LoopBackEmitter implements Emitter {
    private LoopBackListener listener;

    @Inject
    SerializerDeserializer serDeser;

    public LoopBackEmitter(LoopBackListener listener) {
        this.listener = listener;
    }

    @Override
    public boolean send(int partitionId, EventMessage message) {

        listener.put(serDeser.serialize(message));
        return true;
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
