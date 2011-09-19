package io.s4.comm;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueingListener implements Listener, Runnable {
    private Listener listener;
    private BlockingQueue<byte[]> queue;
    private long dropCount = 0;
    private volatile Thread thread;
    
    public QueueingListener(Listener listener, int queueSize) {
        this.listener = listener;
        queue = new LinkedBlockingQueue<byte[]>(queueSize);
    }
    
    public long getDropCount() {
        return dropCount;
    }
    
    public void start() {
        if (thread != null) {
            throw new IllegalStateException("QueueingListener is already started");
        }
        thread = new Thread(this, "QueueingListener");
        thread.start();
    }
    
    public void stop() {
        if (thread == null) {
            throw new IllegalStateException("QueueingListener is already stopped");
        }
        thread.interrupt();
        thread = null;
    }

    @Override
    public byte[] recv() {
        try {
            //System.out.println("QueueingListener: About to take message from queue");
            return queue.take();
        }
        catch (InterruptedException ie) {
            return null;
        }
    }
    
    public int getPartitionId() {
        return listener.getPartitionId();
    }
    
    public void run() {
        while (!Thread.interrupted()) {
            byte[] message = listener.recv();
            if (!queue.offer(message)) {
                dropCount++;
            }
            else {
                //System.out.println("QueueingListener: Adding message of size " + message.length + " to queue");
            }
        }
    }
}
