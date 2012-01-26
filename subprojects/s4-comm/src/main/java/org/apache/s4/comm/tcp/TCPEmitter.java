package org.apache.s4.comm.tcp;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Emitter;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyChangeListener;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
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
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * <p>
 * TCPEmitter - Uses TCP to send messages across partitions. It
 * <ul>
 * <li>guarantees message delivery</li>
 * <li>preserves pair-wise message ordering; might end up sending duplicates to ensure the order</li>
 * <li>tolerates topology changes, partition re-mapping and network glitches</li>
 * </ul>
 * </p>
 * 
 * <p>
 * TCPEmitter is designed as follows:
 * <ul>
 * <li>maintains per-partition queue of {@code Message}s</li>
 * <li> <code>send(p, m)</code> queues the message 'm' to partition 'p'</li>
 * <li>a thread-pool from {@link http
 * ://docs.jboss.org/netty/3.2/api/org/jboss/netty/handler/execution/OrderedMemoryAwareThreadPoolExecutor.html} is used
 * to send the messages asynchronously to the appropriate partitions; send operations between a pair of partitions are
 * serialized</li>
 * <li>Each {@code Message} implements the {@link ChannelFutureListener} and listens on the {@link ChannelFuture}
 * corresponding to the send operation</li>
 * <li>On success, the message marks itself as sent; messages marked sent at the head of the queue are removed</li>
 * <li>On failure of a message m, 'm' and all the messages queued after 'm' are resent to preserve message ordering</li>
 * </ul>
 * </p>
 */

public class TCPEmitter implements Emitter, TopologyChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(TCPEmitter.class);

    private final int numRetries;
    private final int retryDelayMs;
    private final int nettyTimeout;
    private final int bufferSize;

    private Topology topology;
    private final ClientBootstrap bootstrap;

    /*
     * debug information
     */
    private volatile int instanceId = 0;
    private volatile long sentMsgCount = 0;
    private volatile long ackRecvdCount = 0;

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
    private PartitionBasedOMATPE sendService;

    @Inject
    public TCPEmitter(Topology topology, @Named("tcp.partition.queue_size") int bufferSize,
            @Named("comm.retries") int retries, @Named("comm.retry_delay") int retryDelay,
            @Named("comm.timeout") int timeout) throws InterruptedException {
        this.numRetries = retries;
        this.retryDelayMs = retryDelay;
        this.nettyTimeout = timeout;
        this.bufferSize = bufferSize;

        this.topology = topology;
        topology.addListener(this);
        int clusterSize = this.topology.getTopology().getNodes().size();

        partitionChannelMap = HashBiMap.create(clusterSize);
        partitionNodeMap = HashBiMap.create(clusterSize);
        sendQueues = new Hashtable<Integer, SendQueue>(clusterSize);
        sendService = new PartitionBasedOMATPE();

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
        bootstrap.setOption("connectTimeoutMillis", this.nettyTimeout);
    }

    private class PartitionBasedOMATPE extends OrderedMemoryAwareThreadPoolExecutor {
        public PartitionBasedOMATPE() {
            /*
             * Create ordered thread pool - memory limit is enforced by the per-partition queues
             */
            super(getPartitionCount(), 0, 0, nettyTimeout, TimeUnit.MILLISECONDS, new ThreadFactoryBuilder()
                    .setNameFormat("TCPEmitterSendServiceThread-#" + instanceId++ + "-%d")
                    .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread paramThread, Throwable paramThrowable) {
                            logger.error("Cannot send message", paramThrowable);
                        }
                    }).build());
        }

        @Override
        protected ConcurrentMap<Object, Executor> newChildExecutorMap() {
            return new ConcurrentHashMap<Object, Executor>();
        }

        @Override
        protected Object getChildExecutorKey(ChannelEvent e) {
            return partitionChannelMap.inverse().get(e.getChannel());
        }

        @Override
        public boolean removeChildExecutor(Object key) {
            return super.removeChildExecutor(key);
        }
    }

    private class Message implements ChannelFutureListener {
        private final SendQueue sendQ;
        private final byte[] message;
        private boolean sendSuccess = false;

        Message(SendQueue sendQ, byte[] message) {
            this.sendQ = sendQ;
            this.message = message;
        }

        private void sendMessage() {
            sendQ.emitter.sendMessage(sendQ.partitionId, this);
            sentMsgCount++;
        }

        private void messageSendFailure() {
            synchronized (sendQ.failureFound) {
                sendQ.failureFound = true;
            }
            closeAndRemoveChannel(sendQ.partitionId);
            sendQ.spawnSendTask();
        }

        @Override
        public synchronized void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                ackRecvdCount++;
                sendSuccess = true;
                sendQ.clearWire();
                return;
            }

            if (future.isCancelled()) {
                logger.error("Send I/O cancelled to " + future.getChannel().getRemoteAddress());
            }

            // failed operation
            messageSendFailure();
        }
    }

    private class SendQueue {
        private final TCPEmitter emitter;
        private final int partitionId;
        private final int bufferCapacity;
        private final Queue<Message> pending; // messages to be sent
        private final Queue<Message> wire; // messages in transit

        private Integer bufferSize = 0;
        private Boolean failureFound = false;

        SendQueue(TCPEmitter emitter, int partitionId, int bufferCapacity) {
            this.emitter = emitter;
            this.partitionId = partitionId;
            this.bufferCapacity = bufferCapacity;
            this.pending = new ConcurrentLinkedQueue<Message>();
            this.wire = new ConcurrentLinkedQueue<Message>();
        }

        private boolean offer(byte[] message) {
            Message m = new Message(this, message);
            synchronized (bufferSize) {
                if (bufferSize >= bufferCapacity) {
                    return false;
                }

                pending.add(m);
                bufferSize++;
                spawnSendTask();
                return true;
            }
        }

        public void clearWire() {
            while (!wire.isEmpty()) {
                Message msg = wire.peek();
                if (!msg.sendSuccess)
                    return;
                wire.remove();
                synchronized (bufferSize) {
                    bufferSize--;
                }
            }
        }

        private void spawnSendTask() {
            emitter.sendService.execute(new SendTask(this));
        }

        private void resendWiredMessages() {
            clearWire();
            for (Message msg : wire) {
                msg.sendMessage();
            }
        }

        private void sendPendingMessages() {
            while (!pending.isEmpty()) {
                Message msg = pending.remove();
                msg.sendMessage();
                wire.add(msg);
            }
        }

        private void sendMessages() {
            while (true) {
                boolean resend = false;
                synchronized (failureFound) {
                    if (failureFound) {
                        resend = true;
                        failureFound = false;
                    } else
                        break;
                }

                if (resend)
                    resendWiredMessages();
            }

            sendPendingMessages();
        }
    }

    private static class SendTask implements Runnable {
        private final SendQueue sendQ;

        SendTask(SendQueue sendQ) {
            this.sendQ = sendQ;
        }

        @Override
        public void run() {
            sendQ.sendMessages();
        }
    }

    private boolean connectTo(Integer partitionId) {
        ClusterNode clusterNode = partitionNodeMap.get(partitionId);

        if (clusterNode == null) {
            logger.error("No ClusterNode exists for partitionId " + partitionId);
            onChange();
            return false;
        }

        try {
            ChannelFuture f = this.bootstrap.connect(new InetSocketAddress(clusterNode.getMachineName(), clusterNode
                    .getPort()));
            f.await(nettyTimeout);
            if (f.isSuccess()) {
                partitionChannelMap.forcePut(partitionId, f.getChannel());
                return true;
            }
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
            logger.error(String.format("Interrupted while connecting to %s:%d", clusterNode.getMachineName(),
                    clusterNode.getPort()));
        }
        return false;
    }

    private void sendMessage(int partitionId, Message m) {
        boolean messageSent = false;
        ChannelBuffer buffer = ChannelBuffers.buffer(m.message.length);
        buffer.writeBytes(m.message);

        for (int retries = 0; retries < numRetries; retries++) {
            if (!partitionChannelMap.containsKey(partitionId)) {
                if (!connectTo(partitionId)) {
                    continue;
                }
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
                messageSent = true;
                break;
            }
        }

        if (!messageSent) {
            m.messageSendFailure();
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
        // debug
        logger.debug("Sent {} messages, and received ACKs for {} messages", sentMsgCount, ackRecvdCount);

        for (SendQueue sendQ : sendQueues.values()) {
            if (!sendQ.wire.isEmpty()) {
                logger.error("TCPEmitter could not deliver {} messages to partition {}", sendQ.wire.size(),
                        sendQ.partitionId);
                sendQ.wire.clear();
            }

            if (!sendQ.pending.isEmpty()) {
                logger.error("TCPEmitter could not send {} messages to partition {}", sendQ.pending.size(),
                        sendQ.partitionId);
                sendQ.pending.clear();
            }
        }

        ChannelGroup cg = new DefaultChannelGroup();
        synchronized (partitionChannelMap) {
            cg.addAll(partitionChannelMap.values());
            partitionChannelMap.clear();
        }
        try {
            cg.close().await(nettyTimeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // bootstrap.releaseExternalResources();
    }

    protected void closeAndRemoveChannel(int partition) {
        SendQueue sendQ = sendQueues.get(partition);

        if (sendQ == null)
            return;

        synchronized (sendQ) {
            Channel c = partitionChannelMap.remove(partition);
            sendService.removeChildExecutor(partition);
            if (c != null) {
                try {
                    c.close().await(nettyTimeout);
                    c.disconnect().await(nettyTimeout);
                } catch (InterruptedException e) {
                    logger.error("Could not close channel to partition {} gracefully", partition);
                    e.printStackTrace();
                }
            }
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
                closeAndRemoveChannel(partition);
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
            Channel c = context.getChannel();
            Integer partitionId = partitionChannelMap.inverse().get(c);
            if (partitionId == null) {
                return;
            }
            logger.error("Error on channel to partition " + partitionId);
            closeAndRemoveChannel(partitionId);

            SendQueue sendQ = sendQueues.get(partitionId);
            synchronized (sendQ.failureFound) {
                sendQ.failureFound = true;
            }
            sendQ.spawnSendTask();

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
