package org.apache.s4.comm.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import org.apache.s4.base.Emitter;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyChangeListener;

import com.google.inject.Inject;


public class UDPEmitter implements Emitter, TopologyChangeListener {
    private DatagramSocket socket;
    private ClusterNode[] partitions;
    private Map<Integer, InetAddress> inetCache = new HashMap<Integer, InetAddress>();
    private long messageDropInQueueCount = 0;
    private Topology topology;
    
    public long getMessageDropInQueueCount() {
        return messageDropInQueueCount;
    }
    
    @Inject
    public UDPEmitter(Topology topology) {
        this.topology = topology;
        topology.addListener(this);
        partitions = new ClusterNode[topology.getTopology().getNodes().size()];
        for (ClusterNode node : topology.getTopology().getNodes()) {
            partitions[node.getPartition()] = node;
        }
        
        try {
            socket = new DatagramSocket();
        } catch (SocketException se) {
            throw new RuntimeException(se);
        }
    }
    
    public void send(int partitionId, byte[] message) {
        try {
            ClusterNode node = null;
            if (partitionId < partitions.length) {
                node = partitions[partitionId];
            }
            else {
                throw new RuntimeException(String.format("Bad partition id %d", partitionId));
            }
            byte[] byteBuffer = new byte[message.length];
            System.arraycopy(message, 0, byteBuffer, 0, message.length);
            InetAddress inetAddress = inetCache.get(partitionId);
            if (inetAddress == null) {
                inetAddress = InetAddress.getByName(node.getMachineName());
                inetCache.put(partitionId, inetAddress);
            }
            DatagramPacket dp = new DatagramPacket(byteBuffer,
                    byteBuffer.length, inetAddress, node.getPort());
            socket.send(dp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getPartitionCount() {
        return topology.getTopology().getPartitionCount();
    }
    
    public void onChange() {
        // do nothing on change of Topology, for now
    }
}
