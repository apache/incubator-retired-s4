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

import org.apache.s4.base.Destination;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
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
import com.google.inject.name.Named;

/**
 * TCPEmitter - Uses TCP to send messages across partitions.
 * <p>
 * It maintains a mapping of partition to channel, updated upon cluster updates.
 * <p>
 * A throttling mechanism is also provided, so that back pressure can be applied if consumers are too slow.
 * 
 */

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
     * Channel used to send messages to each destination
     */
    private final BiMap<Destination, Channel> destinationChannelMap;

    // lock for synchronizing between cluster updates callbacks and other code
    private final Lock lock;

    @Inject
    SerializerDeserializerFactory serDeserFactory;
    SerializerDeserializer serDeser;
    Map<Destination, Semaphore> writePermits = Maps.newHashMap();

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
       // int clusterSize = this.topology.getPhysicalCluster().getNodes().size();
        destinationChannelMap = HashBiMap.create();

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
        this.topology.addListener(this);
        serDeser = serDeserFactory.createSerializerDeserializer(Thread.currentThread().getContextClassLoader());
        metrics = new EmitterMetrics(topology);

    }

    private boolean connectTo(TCPDestination destination) throws InterruptedException {

        if (destination == null) {
            return false;
        }

        try {
            ChannelFuture connectFuture = this.bootstrap.connect(new InetSocketAddress(destination.getMachineName(),
                    destination.getPort()));
            connectFuture.await();
            if (connectFuture.isSuccess()) {
                channels.add(connectFuture.getChannel());
                destinationChannelMap.forcePut(destination, connectFuture.getChannel());
                writePermits.put(destination, new Semaphore(maxPendingWrites));
                return true;
            }
        } catch (InterruptedException ie) {
            logger.error(String.format("Interrupted while connecting to %s:%d", destination.getMachineName(),
                    destination.getPort()));
            throw ie;
        }
        return false;
    }

    @Override
    public boolean send(Destination destination, ByteBuffer message) throws InterruptedException {
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(message);

        if (!destinationChannelMap.containsKey(destination)) {
            if (!connectTo((TCPDestination) destination)) {
                logger.warn("Could not connect to destination {}, discarding message", destination);
                // Couldn't connect, discard message
                return false;
            }
        }

        writePermits.get(destination).acquire();

        Channel c = destinationChannelMap.get(destination);
        if (c == null) {
            logger.warn("Could not find channel for destination {}", destination);
            return false;
        }

        c.write(buffer).addListener(new MessageSendingListener(destination));
        return true;
    }

    protected void removeChannel(Destination destination) {
        Channel c = destinationChannelMap.remove(destination);
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
            // // dropped destinations are those in local map but not in updated cluster config
            // SetView<Destination> droppedDestinations = Sets.difference(destinationChannelMap.keySet(), Sets
            // .newHashSet(Collections2.transform(topology.getPhysicalCluster().getNodes(),
            // new Function<ClusterNode, Destination>() {
            //
            // @Override
            // public Destination apply(ClusterNode clusterNode) {
            // return new TCPDestination(clusterNode);
            // }
            // })));
            // for (Destination dropped : droppedDestinations) {
            // destinationChannelMap.remove(dropped);
            // writePermits.remove(dropped);
            // removeChannel(dropped);
            // }
            //
            // for (ClusterNode clusterNode : topology.getPhysicalCluster().getNodes()) {
            // Destination destination = new TCPDestination(clusterNode);
            // if (!destinationChannelMap.containsKey(destination)) {
            // destinationChannelMap.put(new TCPDestination(clusterNode), null);
            // writePermits.put(destination, new Semaphore(maxPendingWrites));
            // }
            //
            // }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getPartitionCount(String streamName) {
        return topology.getPhysicalCluster().getPartitionCount(streamName);
    }

    class ExceptionHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            Throwable t = e.getCause();
            if (t instanceof ClosedChannelException) {
                destinationChannelMap.inverse().remove(e.getChannel());
                return;
            } else if (t instanceof ConnectException) {
                destinationChannelMap.inverse().remove(e.getChannel());
                return;
            } else {
                logger.error("Unexpected exception", t);
            }
        }
    }

    class MessageSendingListener implements ChannelFutureListener {

        Destination destination = null;

        public MessageSendingListener(Destination destination) {
            super();
            this.destination = destination;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            writePermits.get(destination).release();
            if (!future.isSuccess()) {
                try {
                    // TODO handle possible cluster reconfiguration between send and failure callback
                    logger.warn("Failed to send message to node {} (according to current cluster information)",
                            destination);
                } catch (IndexOutOfBoundsException ignored) {
                    logger.error("Failed to send message to partition {}", destination);
                    // cluster was changed
                }
            } else {
                metrics.sentMessage(destination);
            }
        }
    }

    @Override
    public String getType() {
        return "tcp";
    }
}
