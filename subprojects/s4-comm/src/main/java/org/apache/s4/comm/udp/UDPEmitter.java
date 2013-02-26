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
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
import org.apache.s4.comm.topology.ClusterNode;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * UDP based emitter.
 * 
 */
@Singleton
public class UDPEmitter implements Emitter, ClusterChangeListener {
    private DatagramSocket socket;
    private final HashBiMap<Integer, ClusterNode> nodes;
    private final Map<Integer, InetAddress> inetCache = new HashMap<Integer, InetAddress>();
    private final long messageDropInQueueCount = 0;
    private final Cluster topology;

    @Inject
    SerializerDeserializerFactory serDeserFactory;
    SerializerDeserializer serDeser;

    public long getMessageDropInQueueCount() {
        return messageDropInQueueCount;
    }

    @Inject
    public UDPEmitter(Cluster topology) {
        this.topology = topology;
        nodes = HashBiMap.create(topology.getPhysicalCluster().getNodes().size());
        for (ClusterNode node : topology.getPhysicalCluster().getNodes()) {
            nodes.forcePut(node.getPartition(), node);
        }

        try {
            socket = new DatagramSocket();
        } catch (SocketException se) {
            throw new RuntimeException(se);
        }
    }

    @Inject
    private void init() {
        serDeser = serDeserFactory.createSerializerDeserializer(Thread.currentThread().getContextClassLoader());
        topology.addListener(this);
        refreshCluster();
    }

    @Override
    public boolean send(int partitionId, ByteBuffer message) {
        try {
            ClusterNode node = nodes.get(partitionId);
            if (node == null) {
                LoggerFactory.getLogger(getClass()).error(
                        "Cannot send message to partition {} because this partition is not visible to this emitter",
                        partitionId);
                return false;
            }
            byte[] byteBuffer = new byte[message.array().length];
            System.arraycopy(message.array(), 0, byteBuffer, 0, message.array().length);
            InetAddress inetAddress = inetCache.get(partitionId);
            if (inetAddress == null) {
                inetAddress = InetAddress.getByName(node.getMachineName());
                inetCache.put(partitionId, inetAddress);
            }
            DatagramPacket dp = new DatagramPacket(byteBuffer, byteBuffer.length, inetAddress, node.getPort());
            socket.send(dp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public int getPartitionCount() {
        return topology.getPhysicalCluster().getPartitionCount();
    }

    @Override
    public void onChange() {
        refreshCluster();
    }

    private void refreshCluster() {
        // topology changes when processes pick tasks
        synchronized (nodes) {
            for (ClusterNode clusterNode : topology.getPhysicalCluster().getNodes()) {
                Integer partition = clusterNode.getPartition();
                nodes.forcePut(partition, clusterNode);
            }
        }

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
}
