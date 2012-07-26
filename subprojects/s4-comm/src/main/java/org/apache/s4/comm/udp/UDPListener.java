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
    }

    @Inject
    private void init() {
        (new Thread(this)).start();
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                socket.receive(datagram);
                byte[] data = new byte[datagram.getLength()];
                System.arraycopy(datagram.getData(), datagram.getOffset(), data, 0, data.length);
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
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public int getPartitionId() {
        return node.getPartition();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
