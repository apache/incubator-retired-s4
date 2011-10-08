package io.s4.comm.loopback;

import io.s4.base.Listener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public class LoopBackListener implements Listener {
    
    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();

    @Override
    public byte[] recv() {
        try {
            //System.out.println("LoopBackListener: Taking message from handoff queue");
            return handoffQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPartitionId() {
        return 0;
    }
    
    public void put(byte[] message) {
        try {
            //System.out.println("LoopBackListener: putting message into handoffqueue");
            handoffQueue.put(message);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }        
    }

}
