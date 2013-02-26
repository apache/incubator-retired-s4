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

package org.apache.s4.comm.tcp;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.util.EmitterMetrics;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * TCPEmitter - Uses TCP to send messages across partitions.
 * <p>
 * It maintains a mapping of partition to channel, updated upon cluster updates.
 * <p>
 * A throttling mechanism is also provided, so that back pressure can be applied if consumers are too slow.
 * 
 */
@Singleton
public class TCPEmitter implements Emitter, ClusterChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(TCPEmitter.class);

    private final int nettyTimeout;

    private Cluster topology;
    private final ClientBootstrap bootstrap;

    /*
     * All channels
     */
    private final ChannelGroup channels = new DefaultChannelGroup();

    /*
     * Channel used to send messages to each partition
     */
    private final BiMap<Integer, Channel> partitionChannelMap;

    /*
     * Node hosting each partition
     */
    private final BiMap<Integer, ClusterNode> partitionNodeMap;

    // lock for synchronizing between cluster updates callbacks and other code
    private final Lock lock;

    @Inject
    SerializerDeserializerFactory serDeserFactory;
    SerializerDeserializer serDeser;
    Map<Integer, Semaphore> writePermits = Maps.newHashMap();

    EmitterMetrics metrics;

    final private int maxPendingWrites;

    /**
     * 
     * @param topology
     *            the target cluster configuration
     * @param timeout
     *            netty timeout
     * @param maxPendingWrites
     *            maximum number of events not yet flushed to the TCP buffer
     * @throws InterruptedException
     *             in case of an interruption
     */
    @Inject
    public TCPEmitter(Cluster topology, @Named("s4.comm.timeout") int timeout,
            @Named("s4.emitter.maxPendingWrites") int maxPendingWrites) throws InterruptedException {
        this.nettyTimeout = timeout;
        this.topology = topology;
        this.maxPendingWrites = maxPendingWrites;
        this.lock = new ReentrantLock();

        // Initialize data structures
        int clusterSize = this.topology.getPhysicalCluster().getNodes().size();
        partitionChannelMap = HashBiMap.create(clusterSize);
        partitionNodeMap = HashBiMap.create(clusterSize);

        // Initialize netty related structures
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldPrepender(4));
                p.addLast("2", new ExceptionHandler());
                return p;
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", this.nettyTimeout);

    }

    @Inject
    private void init() {
        refreshCluster();
        this.topology.addListener(this);
        serDeser = serDeserFactory.createSerializerDeserializer(Thread.currentThread().getContextClassLoader());
        metrics = new EmitterMetrics(topology);

    }

    private boolean connectTo(Integer partitionId) throws InterruptedException {
        ClusterNode clusterNode = partitionNodeMap.get(partitionId);

        if (clusterNode == null) {

            logger.error("No ClusterNode exists for partitionId " + partitionId);
            refreshCluster();
            return false;
        }

        try {
            ChannelFuture connectFuture = this.bootstrap.connect(new InetSocketAddress(clusterNode.getMachineName(),
                    clusterNode.getPort()));
            connectFuture.await();
            if (connectFuture.isSuccess()) {
                channels.add(connectFuture.getChannel());
                partitionChannelMap.forcePut(partitionId, connectFuture.getChannel());
                return true;
            }
        } catch (InterruptedException ie) {
            logger.error(String.format("Interrupted while connecting to %s:%d", clusterNode.getMachineName(),
                    clusterNode.getPort()));
            throw ie;
        }
        return false;
    }

    private void sendMessage(int partitionId, ByteBuffer message) throws InterruptedException {
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(message);

        if (!partitionChannelMap.containsKey(partitionId)) {
            if (!connectTo(partitionId)) {
                logger.warn("Could not connect to partition {}, discarding message", partitionId);
                // Couldn't connect, discard message
                return;
            }
        }

        writePermits.get(partitionId).acquire();

        Channel c = partitionChannelMap.get(partitionId);
        if (c == null) {
            logger.warn("Could not find channel for partition {}", partitionId);
            return;
        }

        c.write(buffer).addListener(new MessageSendingListener(partitionId));
    }

    @Override
    public boolean send(int partitionId, ByteBuffer message) throws InterruptedException {
        // TODO a possible optimization would be to buffer messages per partition, with a small timeout. This will limit
        // the number of writes and therefore system calls.
        sendMessage(partitionId, message);
        return true;
    }

    protected void removeChannel(int partition) {
        Channel c = partitionChannelMap.remove(partition);
        if (c == null) {
            return;
        }
        c.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess())
                    channels.remove(future.getChannel());
                else
                    logger.error("Failed to close channel");
            }
        });
    }

    @Override
    public void close() {
        try {
            topology.removeListener(this);
            channels.close().await();
            bootstrap.releaseExternalResources();
        } catch (InterruptedException ie) {
            logger.error("Interrupted while closing");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onChange() {
        refreshCluster();
    }

    private void refreshCluster() {
        lock.lock();
        try {
            for (ClusterNode clusterNode : topology.getPhysicalCluster().getNodes()) {
                Integer partition = clusterNode.getPartition();
                if (partition == null) {
                    logger.error("Illegal partition for clusterNode - " + clusterNode);
                    return;
                }

                ClusterNode oldNode = partitionNodeMap.remove(partition);
                if (oldNode != null && !oldNode.equals(clusterNode)) {
                    removeChannel(partition);
                }
                partitionNodeMap.forcePut(partition, clusterNode);
                if (!writePermits.containsKey(partition)) {
                    writePermits.put(partition, new Semaphore(maxPendingWrites));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getPartitionCount() {
        return topology.getPhysicalCluster().getPartitionCount();
    }

    class ExceptionHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            Throwable t = e.getCause();
            if (t instanceof ClosedChannelException) {
                partitionChannelMap.inverse().remove(e.getChannel());
                return;
            } else if (t instanceof ConnectException) {
                partitionChannelMap.inverse().remove(e.getChannel());
                return;
            } else {
                logger.error("Unexpected exception", t);
            }
        }
    }

    class MessageSendingListener implements ChannelFutureListener {

        int partitionId = -1;

        public MessageSendingListener(int partitionId) {
            super();
            this.partitionId = partitionId;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            writePermits.get(partitionId).release();
            if (!future.isSuccess()) {
                try {
                    // TODO handle possible cluster reconfiguration between send and failure callback
                    logger.warn("Failed to send message to node {} (according to current cluster information)",
                            topology.getPhysicalCluster().getNodes().get(partitionId));
                } catch (IndexOutOfBoundsException ignored) {
                    logger.error("Failed to send message to partition {}", partitionId);
                    // cluster was changed
                }
            } else {
                metrics.sentMessage(partitionId);

            }

        }
    }
}
