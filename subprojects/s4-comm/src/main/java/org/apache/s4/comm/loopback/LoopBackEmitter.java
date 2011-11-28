package org.apache.s4.comm.loopback;

import org.apache.s4.base.Emitter;

public class LoopBackEmitter implements Emitter {
    private LoopBackListener listener;
    
    public LoopBackEmitter(LoopBackListener listener) {
        this.listener = listener;
    }
    
    @Override
    public boolean send(int partitionId, byte[] message) {
        listener.put(message);
        return true;
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

}
