/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.comm.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.s4.base.Listener;
import org.apache.s4.base.Receiver;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.ClusterNode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * 
 * Implementation of a simple UDP listener.
 * 
 */
@Singleton
public class UDPListener implements Listener, Runnable {

    private DatagramSocket socket;
    private final DatagramPacket datagram;
    private final byte[] bs;
    static int BUFFER_LENGTH = 65507;
    private final BlockingQueue<ByteBuffer> handoffQueue = new SynchronousQueue<ByteBuffer>();
    private final ClusterNode node;
    private final Receiver receiver;

    @Inject
    public UDPListener(Assignment assignment, final Receiver receiver) {
        this(assignment, -1, receiver);
    }

    public UDPListener(Assignment assignment, int UDPBufferSize, final Receiver receiver) {
        this.receiver = receiver;
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
    }

    @Inject
    private void init() {
        (new Thread(this)).start();
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                socket.receive(datagram);
                ChannelBuffer copiedBuffer = ChannelBuffers.copiedBuffer(datagram.getData(), datagram.getOffset(),
                        datagram.getLength());
                datagram.setLength(BUFFER_LENGTH);
                receiver.receive(copiedBuffer.toByteBuffer());
                // try {
                // handoffQueue.put(copiedBuffer.toByteBuffer());
                // } catch (InterruptedException ie) {
                // Thread.currentThread().interrupt();
                // }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer recv() {
        try {
            return handoffQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public int getPartitionId() {
        return node.getPartition();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
