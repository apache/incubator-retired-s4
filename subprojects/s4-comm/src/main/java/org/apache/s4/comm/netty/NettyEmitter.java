package org.apache.s4.comm.netty;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.Executors;

import org.apache.s4.base.Emitter;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyChangeListener;
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
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.google.inject.Inject;

public class NettyEmitter implements Emitter, ChannelFutureListener, TopologyChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(NettyEmitter.class);
    private static final int BUFFER_SIZE = 10;
    private static final int NUM_RETRIES = 10;

    private Topology topology;
    private final ClientBootstrap bootstrap;

    static class MessageQueuesPerPartition {
        private Hashtable<Integer, Queue<byte[]>> queues = new Hashtable<Integer, Queue<byte[]>>();
        private boolean bounded;

        MessageQueuesPerPartition(boolean bounded) {
            this.bounded = bounded;
        }

        private boolean add(int partitionId, byte[] message) {
            Queue<byte[]> messages = queues.get(partitionId);

            if (messages == null) {
                messages = new ArrayDeque<byte[]>();
                queues.put(partitionId, messages);
            }

            if (bounded && messages.size() >= BUFFER_SIZE) {
                // Too many messages already queued
                return false;
            }

            messages.offer(message);
            return true;
        }

        private byte[] peek(int partitionId) {
            Queue<byte[]> messages = queues.get(partitionId);

            try {
                return messages.peek();
            } catch (NullPointerException npe) {
                return null;
            }
        }

        private void remove(int partitionId) {
            Queue<byte[]> messages = queues.get(partitionId);

            if (messages.isEmpty()) {
                logger.error("Trying to remove messages from an empty queue for partition" + partitionId);
                return;
            }

            if (messages != null)
                messages.remove();
        }
    }

    private HashBiMap<Integer, Channel> partitionChannelMap;
    private HashBiMap<Integer, ClusterNode> partitionNodeMap;
    private MessageQueuesPerPartition queuedMessages = new MessageQueuesPerPartition(true);

    @Inject
    public NettyEmitter(Topology topology) throws InterruptedException {
        this.topology = topology;
        topology.addListener(this);
        int clusterSize = this.topology.getTopology().getNodes().size();

        partitionChannelMap = HashBiMap.create(clusterSize);
        partitionNodeMap = HashBiMap.create(clusterSize);

        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldPrepender(4));
                p.addLast("2", new TestHandler());
                return p;
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
    }

    private boolean connectTo(Integer partitionId) {
        ClusterNode clusterNode = partitionNodeMap.get(partitionId);

        if (clusterNode == null) {
            clusterNode = topology.getTopology().getNodes().get(partitionId);
            partitionNodeMap.forcePut(partitionId, clusterNode);
        }

        if (clusterNode == null) {
            logger.error("No ClusterNode exists for partitionId " + partitionId);
            return false;
        }

        for (int retries = 0; retries < NUM_RETRIES; retries++) {
            ChannelFuture f = this.bootstrap.connect(new InetSocketAddress(clusterNode.getMachineName(), clusterNode
                    .getPort()));
            f.awaitUninterruptibly();
            if (f.isSuccess()) {
                partitionChannelMap.forcePut(partitionId, f.getChannel());
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                logger.error(String.format("Interrupted while connecting to %s:%d", clusterNode.getMachineName(),
                        clusterNode.getPort()));
            }
        }

        return false;
    }

    private void writeMessageToChannel(Channel channel, int partitionId, byte[] message) {
        ChannelBuffer buffer = ChannelBuffers.buffer(message.length);
        buffer.writeBytes(message);
        ChannelFuture f = channel.write(buffer);
        f.addListener(this);
    }

    private final Object sendLock = new Object();

    @Override
    public boolean send(int partitionId, byte[] message) {
        Channel channel = partitionChannelMap.get(partitionId);
        if (channel == null) {
            if (connectTo(partitionId)) {
                channel = partitionChannelMap.get(partitionId);
            } else {
                // could not connect, queue to the partitionBuffer
                return queuedMessages.add(partitionId, message);
            }
        }

        /*
         * Try limiting the size of the send queue inside Netty
         */
        if (!channel.isWritable()) {
            synchronized (sendLock) {
                // check again now that we have the lock
                while (!channel.isWritable()) {
                    try {
                        sendLock.wait();
                    } catch (InterruptedException ie) {
                        return false;
                    }
                }
            }
        }

        /*
         * Channel is available. Write messages in the following order: (1) Messages already on wire, (2) Previously
         * buffered messages, and (3) the Current Message
         * 
         * Once the channel returns success delete from the messagesOnTheWire
         */
        byte[] messageBeingSent = null;
        // while ((messageBeingSent = messagesOnTheWire.peek(partitionId)) != null) {
        // writeMessageToChannel(channel, partitionId, messageBeingSent, false);
        // }

        while ((messageBeingSent = queuedMessages.peek(partitionId)) != null) {
            writeMessageToChannel(channel, partitionId, messageBeingSent);
            queuedMessages.remove(partitionId);
        }

        writeMessageToChannel(channel, partitionId, message);
        return true;
    }

    @Override
    public void operationComplete(ChannelFuture f) {
        int partitionId = partitionChannelMap.inverse().get(f.getChannel());
        if (f.isSuccess()) {
            // messagesOnTheWire.remove(partitionId);
        }

        if (f.isCancelled()) {
            logger.error("Send I/O was cancelled!! " + f.getChannel().getRemoteAddress());
        } else if (!f.isSuccess()) {
            logger.error("Exception on I/O operation", f.getCause());
            logger.error(String.format("I/O on partition %d failed!", partitionId));
            partitionChannelMap.remove(partitionId);
        }
    }

    @Override
    public void onChange() {
        /*
         * Close the channels that correspond to changed partitions and update partitionNodeMap
         */
        for (ClusterNode clusterNode : topology.getTopology().getNodes()) {
            Integer partition = clusterNode.getPartition();
            ClusterNode oldNode = partitionNodeMap.get(partition);

            if (oldNode != null && !oldNode.equals(clusterNode)) {
                partitionChannelMap.remove(partition).close();
            }

            partitionNodeMap.forcePut(partition, clusterNode);
        }
    }

    @Override
    public int getPartitionCount() {
        // Number of nodes is not same as number of partitions
        return topology.getTopology().getPartitionCount();
    }

    class TestHandler extends SimpleChannelHandler {
        @Override
        public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
            // logger.info(String.format("%08x %08x %08x", e.getValue(),
            // e.getChannel().getInterestOps(), Channel.OP_WRITE));
            synchronized (sendLock) {
                if (e.getChannel().isWritable()) {
                    sendLock.notify();
                }
            }
            ctx.sendUpstream(e);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            Integer partitionId = partitionChannelMap.inverse().get(context.getChannel());
            if (partitionId == null) {
                logger.error("Error on mystery channel!!");
            }
            logger.error("Error on channel to partition " + partitionId);

            try {
                throw event.getCause();
            } catch (ConnectException ce) {
                logger.error(ce.getMessage(), ce);
            } catch (Throwable err) {
                logger.error("Error", err);
                if (context.getChannel().isOpen()) {
                    logger.error("Closing channel due to exception");
                    context.getChannel().close();
                }
            }
        }
    }
}
