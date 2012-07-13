package org.apache.s4.comm.tcp;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
import org.apache.s4.comm.topology.ClusterNode;
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
 * <p>
 * TCPEmitter - Uses TCP to send messages across partitions. It
 * <ul>
 * <li>guarantees message delivery</li>
 * <li>preserves pair-wise message ordering; might end up sending duplicates to
 * ensure the order</li>
 * <li>tolerates topology changes, partition re-mapping and network glitches</li>
 * </ul>
 * </p>
 * 
 * <p>
 * TCPEmitter is designed as follows:
 * <ul>
 * <li>maintains per-partition queue of {@code Message}s</li>
 * <li> <code>send(p, m)</code> queues the message 'm' to partition 'p'</li>
 * <li>a thread-pool is used to send the messages asynchronously to the
 * appropriate partitions; send operations between a pair of partitions are
 * serialized</li>
 * <li>Each {@code Message} implements the {@link ChannelFutureListener} and
 * listens on the {@link ChannelFuture} corresponding to the send operation</li>
 * <li>On success, the message marks itself as sent; messages marked sent at the
 * head of the queue are removed</li>
 * <li>On failure of a message m, 'm' and all the messages queued after 'm' are
 * resent to preserve message ordering</li>
 * </ul>
 * </p>
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
    SerializerDeserializer serDeser;

    @Inject
    public TCPEmitter(Cluster topology, @Named("comm.timeout") int timeout) throws InterruptedException {
        this.nettyTimeout = timeout;
        this.topology = topology;
        this.lock = new ReentrantLock();

        // Initialize data structures
        int clusterSize = this.topology.getPhysicalCluster().getNodes().size();
        partitionChannelMap = Maps.synchronizedBiMap(HashBiMap.<Integer, Channel> create(clusterSize));
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
        this.topology.addListener(this);
        refreshCluster();
    }

    private boolean connectTo(Integer partitionId) {
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
        }
        return false;
    }

    private void sendMessage(int partitionId, byte[] message) {
        ChannelBuffer buffer = ChannelBuffers.buffer(message.length);
        buffer.writeBytes(message);

        if (!partitionChannelMap.containsKey(partitionId)) {
            if (!connectTo(partitionId)) {
                // Couldn't connect, discard message
                return;
            }
        }

        Channel c = partitionChannelMap.get(partitionId);
        if (c == null)
            return;

        c.write(buffer);
    }

    @Override
    public boolean send(int partitionId, EventMessage message) {
        sendMessage(partitionId, serDeser.serialize(message));
        return true;
    }

    protected void removeChannel(int partition) {
        Channel c = partitionChannelMap.remove(partition);
        if (c == null)
            return;

        c.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess())
                    channels.remove(future.getChannel());
                else
                    logger.error("FAILED to close channel");
            }
        });
    }

    public void close() {
        try {
            channels.close().await();
            bootstrap.releaseExternalResources();
        } catch (InterruptedException ie) {
            logger.error("Interrupted while closing");
            ie.printStackTrace();
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
                    logger.error("onChange(): Illegal partition for clusterNode - " + clusterNode);
                    return;
                }

                ClusterNode oldNode = partitionNodeMap.remove(partition);
                if (oldNode != null && !oldNode.equals(clusterNode)) {
                    removeChannel(partition);
                }
                partitionNodeMap.forcePut(partition, clusterNode);
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
}
