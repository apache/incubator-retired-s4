package org.apache.s4.comm.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.s4.base.Listener;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.ClusterNode;

import com.google.inject.Inject;


/**
 * 
 * Implementation of a simple UDP listener.
 *
 */
public class UDPListener implements Listener, Runnable {

    private DatagramSocket socket;
    private DatagramPacket datagram;
    private byte[] bs;
    static int BUFFER_LENGTH = 65507;
    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();
    private ClusterNode node;

    @Inject
    public UDPListener(Assignment assignment) {
        this(assignment, -1);
    }

    public UDPListener(Assignment assignment, int UDPBufferSize) {
        // wait for an assignment
        node = assignment.assignClusterNode();

        try {
            socket = new DatagramSocket(node.getPort());
            if (UDPBufferSize > 0) {
                socket.setReceiveBufferSize(UDPBufferSize);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        bs = new byte[BUFFER_LENGTH];
        datagram = new DatagramPacket(bs, bs.length);
        (new Thread(this)).start();
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                socket.receive(datagram);
                byte[] data = new byte[datagram.getLength()];
                System.arraycopy(datagram.getData(), datagram.getOffset(),
                        data, 0, data.length);
                datagram.setLength(BUFFER_LENGTH);
                try {
                    handoffQueue.put(data);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] recv() {
        try {
            return handoffQueue.take();
        } catch (InterruptedException e) {
        	return null;
        }
    }

    public int getPartitionId() {
        return node.getPartition();
    }

}
