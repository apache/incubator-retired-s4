package org.apache.s4.comm.loopback;

import org.apache.s4.base.Emitter;

public class LoopBackEmitter implements Emitter {
    private LoopBackListener listener;
    
    public LoopBackEmitter(LoopBackListener listener) {
        this.listener = listener;
    }
    
    @Override
    public void send(int partitionId, byte[] message) {
        //System.out.println("LoopBackEmitter: Putting message to listener");
        listener.put(message);
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

}
