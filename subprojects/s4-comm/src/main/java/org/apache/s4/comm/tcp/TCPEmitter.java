package org.apache.s4.comm.tcp;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
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
 * <li>a thread-pool is used to send the messages asynchronously to the appropriate partitions; send operations between
 * a pair of partitions are serialized</li>
 * <li>Each {@code Message} implements the {@link ChannelFutureListener} and listens on the {@link ChannelFuture}
 * corresponding to the send operation</li>
 * <li>On success, the message marks itself as sent; messages marked sent at the head of the queue are removed</li>
 * <li>On failure of a message m, 'm' and all the messages queued after 'm' are resent to preserve message ordering</li>
 * </ul>
 * </p>
 */

public class TCPEmitter implements Emitter, ClusterChangeListener {
    private static final Logger logger = LoggerFactory.getLogger(TCPEmitter.class);

    private final int numRetries;
    private final int retryDelayMs;
    private final int nettyTimeout;
    private final int bufferCapacity;

    private Cluster topology;
    private final ClientBootstrap bootstrap;

    /*
     * debug information
     */
    private volatile int instanceId = 0;

    /*
     * All channels
     */
    private final ChannelGroup channels = new DefaultChannelGroup();

    /*
     * Channel used to send messages to each partition
     */
    private final HashBiMap<Integer, Channel> partitionChannelMap;

    /*
     * Node hosting each partition
     */
    private final HashBiMap<Integer, ClusterNode> partitionNodeMap;

    /*
     * Messages to be sent, stored per partition
     */
    private final Hashtable<Integer, SendQueue> sendQueues;

    /*
     * Thread pool to actually send messages
     */
    private final ExecutorService sendService;

    @Inject
    SerializerDeserializer serDeser;

    // lock for synchronizing between cluster updates callbacks and other code
    private final Lock lock;

    @Inject
    public TCPEmitter(Cluster topology, @Named("tcp.partition.queue_size") int bufferSize,
            @Named("comm.retries") int retries, @Named("comm.retry_delay") int retryDelay,
            @Named("comm.timeout") int timeout) throws InterruptedException {
        this.numRetries = retries;
        this.retryDelayMs = retryDelay;
        this.nettyTimeout = timeout;
        this.bufferCapacity = bufferSize;
        this.topology = topology;
        this.lock = new ReentrantLock();

        // Initialize data structures
        int clusterSize = this.topology.getPhysicalCluster().getNodes().size();
        partitionChannelMap = HashBiMap.create(clusterSize);
        partitionNodeMap = HashBiMap.create(clusterSize);
        sendQueues = new Hashtable<Integer, SendQueue>(clusterSize);

        // Initialize sendService
        int numCores = Runtime.getRuntime().availableProcessors();
        sendService = Executors.newFixedThreadPool(2 * numCores,
                new ThreadFactoryBuilder().setNameFormat("TCPEmitterSendServiceThread-#" + instanceId++ + "-%d")
                        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread paramThread, Throwable paramThrowable) {
                                logger.error("Cannot send message", paramThrowable);
                            }
                        }).build());

        // Initialize netty related structures
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

    @Inject
    private void init() {
        refreshCluster();
        this.topology.addListener(this);
    }

    private class Message implements ChannelFutureListener {
        private final SendQueue sendQ;
        private final byte[] message;
        private boolean sendSuccess = false;
        @Inject
        SerializerDeserializer serDeser;

        Message(SendQueue sendQ, byte[] message) {
            this.sendQ = sendQ;
            this.message = message;
        }

        private void sendMessage() {
            sendQ.emitter.sendMessage(sendQ.partitionId, this);
        }

        private void messageSendFailure() {
            logger.debug("Message send to partition {} has failed", sendQ.partitionId);
            synchronized (sendQ.failureFound) {
                sendQ.failureFound = true;
            }
            removeChannel(sendQ.partitionId);
            sendQ.spawnSendTask();
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
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
        private Boolean sending = false;
        private Boolean failureFound = false;
        private Boolean newMessages = false;

        SendQueue(TCPEmitter emitter, int partitionId, int bufferCapacity) {
            this.emitter = emitter;
            this.partitionId = partitionId;
            this.bufferCapacity = bufferCapacity;
            this.pending = new ConcurrentLinkedQueue<Message>();
            this.wire = new ConcurrentLinkedQueue<Message>();
        }

        private boolean lock() {
            if (sending)
                return false;

            sending = true;
            return true;
        }

        private void unlock() {
            sending = false;
        }

        private boolean offer(byte[] message) {
            Message m = new Message(this, message);
            synchronized (bufferSize) {
                if (bufferSize >= bufferCapacity) {
                    return false;
                }
                bufferSize++;
            }

            pending.add(m);
            spawnSendTask();
            return true;

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
            // Lock before spawning a new SendTask
            boolean acquired = lock();
            if (acquired) {
                try {
                    emitter.sendService.execute(new SendTask(this));
                } finally {
                    unlock();
                }
            } else {
                synchronized (newMessages) {
                    newMessages = true;
                }
            }
        }

        private void resendWiredMessages() {
            clearWire();
            for (Message msg : wire) {
                msg.sendMessage();
            }
        }

        private void sendPendingMessages() {
            Message msg = null;
            while ((msg = pending.poll()) != null) {
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

            while (true) {
                sendPendingMessages();
                synchronized (newMessages) {
                    if (newMessages) {
                        newMessages = false;
                        continue;
                    } else {
                        unlock();
                        break;
                    }
                }
            }
        }
    }

    private class SendTask implements Runnable {
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

            Channel c = partitionChannelMap.get(partitionId);
            if (c == null)
                continue;
            if (!c.isWritable()) {
                try {
                    logger.debug("Waiting for channel to partition {} to become writable", partitionId);
                    // Though we wait for the channel to be writable, it could immediately become non-writable. Hence,
                    // the wait is just a precaution to minimize failed writes.
                    SendQueue sendQ = sendQueues.get(partitionId);
                    synchronized (sendQ) {
                        sendQ.wait();
                    }
                } catch (InterruptedException e) {
                    continue;
                }
            }

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
    public boolean send(int partitionId, EventMessage message) {
        if (!sendQueues.containsKey(partitionId)) {
            SendQueue sendQueue = new SendQueue(this, partitionId, this.bufferCapacity);
            sendQueues.put(partitionId, sendQueue);
        }

        return sendQueues.get(partitionId).offer(serDeser.serialize(message));
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

    class NotifyChannelInterestChange extends SimpleChannelHandler {
        @Override
        public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
            Channel c = e.getChannel();
            Integer partitionId = partitionChannelMap.inverse().get(c);
            if (partitionId == null) {
                logger.debug("channelInterestChanged for an unknown/deleted channel");
                return;
            }

            SendQueue sendQ = sendQueues.get(partitionId);
            synchronized (sendQ) {
                if (c.isWritable()) {
                    sendQ.notify();
                }
            }

            ctx.sendUpstream(e);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            try {
                throw event.getCause();
            } catch (ClosedChannelException cce) {
                return;
            } catch (ConnectException ce) {
                return;
            } catch (Throwable e) {
                e.printStackTrace();
                // Integer partitionId = partitionChannelMap.inverse().get(context.getChannel());
                // String target;
                // if (partitionId == null) {
                // target = "unknown channel";
                // } else {
                // target = "channel for partition [" + partitionId + "], target node host ["
                // + partitionNodeMap.get(partitionId).getMachineName() + "], target node port ["
                // + partitionNodeMap.get(partitionId).getPort() + "]";
                // }
                // logger.error(
                // "Error on [{}]. This can be due to a disconnection of the receiver node. Channel will be closed.",
                // target);
                //
                // if (context.getChannel().isOpen()) {
                // logger.info("Closing channel [{}] due to exception [{}]", target, event.getCause().getMessage());
                // context.getChannel().close();
                // }

            }
        }
    }
}
