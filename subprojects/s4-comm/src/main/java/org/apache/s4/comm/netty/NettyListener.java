package org.apache.s4.comm.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import org.apache.s4.base.Listener;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.ClusterNode;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


public class NettyListener implements Listener {
    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();
    private ClusterNode node;
    private static final Logger logger = LoggerFactory.getLogger(NettyListener.class);
    
    @Inject
    public NettyListener(Assignment assignment) {
        // wait for an assignment
        node = assignment.assignClusterNode();
        
        ChannelFactory factory =
            new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldBasedFrameDecoder(999999, 0, 4, 0, 4));
                p.addLast("2", new ChannelHandler(handoffQueue));
                
                return p;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        
        bootstrap.bind(new InetSocketAddress(node.getPort()));
    }
    
    public byte[] recv() {
        try {
            return handoffQueue.take();
        } catch (InterruptedException e) {
        	return null;
        }
    }
    
    public int getPartitionId() {
        return node.getPartition();
    }
    
    public class ChannelHandler extends SimpleChannelHandler {
        private BlockingQueue<byte[]> handoffQueue;
        
        public ChannelHandler(BlockingQueue<byte[]> handOffQueue) {
            this.handoffQueue = handOffQueue;
        }
        
        public void messageReceived(ChannelHandlerContext ctx,
                MessageEvent e) {
            ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
            try {
                handoffQueue.put(buffer.array()); // this holds up the Netty upstream I/O thread if
                                                  // there's no receiver at the other end of the handoff queue
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            logger.error("Error", event.getCause());
            if (context.getChannel().isOpen()) {
                logger.error("Closing channel due to exception");
                context.getChannel().close();
            }
        }
    }
}
