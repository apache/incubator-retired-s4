package org.apache.s4.comm.tcp;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
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
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * TCPEmitter - Uses TCP to send messages across to other partitions.
 *            - Message ordering between partitions is preserved.
 *            - For efficiency, NettyEmitter.send() queues the messages partition-wise, 
 *              a threadPool sends the messages asynchronously; message dequeued only on success.
 *            - Tolerates topology changes, partition re-mapping, and network glitches.
 */

public class TCPEmitter implements Emitter, TopologyChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(TCPEmitter.class);
    private final int numRetries = 10;
    private final int bufferSize;
    private Topology topology;
    private final ClientBootstrap bootstrap;

    /*
     * Channel used to send messages to each partition
     */
    private HashBiMap<Integer, Channel> partitionChannelMap;

    /*
     * Node hosting each partition
     */
    private HashBiMap<Integer, ClusterNode> partitionNodeMap;

    /*
     * Messages to be sent, stored per partition
     */
    private Hashtable<Integer, SendQueue> sendQueues;

    /*
     * Thread pool to actually send messages
     */
    private ExecutorService sendService = Executors.newCachedThreadPool();

    @Inject
    public TCPEmitter(Topology topology, @Named("tcp.partition.queue_size") int bufferSize) throws InterruptedException {
        this.bufferSize = bufferSize;
        this.topology = topology;
        topology.addListener(this);
        int clusterSize = this.topology.getTopology().getNodes().size();

        partitionChannelMap = HashBiMap.create(clusterSize);
        partitionNodeMap = HashBiMap.create(clusterSize);
        sendQueues = new Hashtable<Integer, SendQueue>(clusterSize);

        /*
         * Initialize netty related structures
         */
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldPrepender(4));
                p.addLast("2", new NotifyChannelInterestChange());
                return p;
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);
    }

    private static class Message implements ChannelFutureListener {
        private final SendQueue sendQ;
        private final byte[] message;
        private boolean sendInProcess;

        Message(SendQueue sendQ, byte[] message) {
            this.sendQ = sendQ;
            this.message = message;
            this.sendInProcess = false;
        }

        private void sendMessage() {
            if (sendInProcess)
                return;

            sendQ.emitter.sendMessage(sendQ.partitionId, this);
            sendInProcess = true;
        }

        @Override
        public synchronized void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                synchronized (sendQ.messages) {
                    sendQ.messages.remove(this);
                }
                return;
            }

            if (future.isCancelled()) {
                logger.error("Send I/O cancelled to " + future.getChannel().getRemoteAddress());
            }

            // failed operation
            sendInProcess = false;
            future.getChannel().close().awaitUninterruptibly();
            sendQ.spawnSendThread();
        }
    }

    private static class SendQueue {
        private final TCPEmitter emitter;
        private final int partitionId;
        private final int bufferSize;
        private Queue<Message> messages;

        private boolean sendThreadRecheck = false;
        private Boolean sendThreadActive = false;

        SendQueue(TCPEmitter emitter, int partitionId, int bufferSize) {
            this.emitter = emitter;
            this.partitionId = partitionId;
            this.bufferSize = bufferSize;
            this.messages = new ArrayBlockingQueue<Message>(this.bufferSize);
        }

        private void spawnSendThread() {
            synchronized (sendThreadActive) {
                if (sendThreadActive) {
                    sendThreadRecheck = true;
                } else {
                    sendThreadActive = true;
                    emitter.sendService.execute(new SendThread(this));
                }
            }
        }

        private boolean offer(byte[] message) {
            Message m = new Message(this, message);
            synchronized (messages) {
                if (messages.offer(m)) {
                    spawnSendThread();
                    return true;
                } else
                    return false;
            }
        }

        private void sendMessagesInQueue() {
            synchronized (messages) {
                for (Message message : messages) {
                    message.sendMessage();
                }
            }
        }
    }

    private static class SendThread extends Thread {
        private final SendQueue sendQ;

        SendThread(SendQueue sendQ) {
            this.sendQ = sendQ;
        }

        @Override
        public void run() {
            while (true) {
                sendQ.sendMessagesInQueue();

                synchronized (sendQ.sendThreadActive) {
                    if (sendQ.sendThreadRecheck) {
                        sendQ.sendThreadRecheck = false;
                        continue;
                    } else {
                        sendQ.sendThreadActive = false;
                        return;
                    }
                }
            }
        }
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

        for (int retries = 0; retries < numRetries; retries++) {
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

    private void sendMessage(int partitionId, Message m) {
        ChannelBuffer buffer = ChannelBuffers.buffer(m.message.length);
        buffer.writeBytes(m.message);

        while (true) {
            if (!partitionChannelMap.containsKey(partitionId)) {
                connectTo(partitionId);
                continue;
            }

            SendQueue sendQ = sendQueues.get(partitionId);
            synchronized (sendQ) {
                if (!partitionChannelMap.get(partitionId).isWritable()) {
                    try {
                        logger.debug("Waiting for channel to partition {} to become writable", partitionId);
                        sendQ.wait();
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
            }

            Channel c = partitionChannelMap.get(partitionId);
            if (c != null && c.isWritable()) {
                c.write(buffer).addListener(m);
                break;
            }
        }
    }

    @Override
    public boolean send(int partitionId, byte[] message) {
        if (!sendQueues.containsKey(partitionId)) {
            SendQueue sendQueue = new SendQueue(this, partitionId, this.bufferSize);
            sendQueues.put(partitionId, sendQueue);
        }

        SendQueue sendQueue = sendQueues.get(partitionId);
        return sendQueue.offer(message);
    }

    public void close() {
        ChannelGroup cg = new DefaultChannelGroup();
        synchronized (partitionChannelMap) {
            cg.addAll(partitionChannelMap.values());
            partitionChannelMap.clear();
        }
        cg.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

    protected void closeChannel(int partition) {
        Channel c = partitionChannelMap.remove(partition);
        if (c != null) {
            c.close().awaitUninterruptibly();
        }
    }

    @Override
    public void onChange() {
        for (ClusterNode clusterNode : topology.getTopology().getNodes()) {
            Integer partition = clusterNode.getPartition();
            if (partition == null) {
                logger.error("onChange(): Illegal partition for clusterNode - " + clusterNode);
                return;
            }

            ClusterNode oldNode = partitionNodeMap.get(partition);

            if (oldNode != null && !oldNode.equals(clusterNode)) {
                closeChannel(partition);
            }

            partitionNodeMap.forcePut(partition, clusterNode);
        }
    }

    @Override
    public int getPartitionCount() {
        // Number of nodes is not same as number of partitions
        return topology.getTopology().getPartitionCount();
    }

    class NotifyChannelInterestChange extends SimpleChannelHandler {
        @Override
        public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
            Channel c = e.getChannel();
            SendQueue sendQ = sendQueues.get(partitionChannelMap.inverse().get(c));
            synchronized (sendQ) {
                if (c.isWritable()) {
                    sendQ.notify();
                }
            }
            ctx.sendUpstream(e);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            Integer partitionId = partitionChannelMap.inverse().get(context.getChannel());
            if (partitionId == null) {
                return;
            }
            logger.error("Error on channel to partition " + partitionId);
            partitionChannelMap.remove(partitionId);

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
