package org.apache.s4.comm.netty;

import java.net.ConnectException;
import java.net.InetSocketAddress;
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

public class NettyEmitter implements Emitter, ChannelFutureListener,
		TopologyChangeListener {
	private static final Logger logger = LoggerFactory
			.getLogger(NettyEmitter.class);

	private Topology topology;
	private final ClientBootstrap bootstrap;

	// Hashtable inherently allows capturing changes to the underlying topology
	private HashBiMap<Integer, Channel> channels;
	private HashBiMap<Integer, ClusterNode> nodes;

	@Inject
	public NettyEmitter(Topology topology) throws InterruptedException {
		this.topology = topology;
		int clusterSize = this.topology.getTopology().getNodes().size();
		
		channels = HashBiMap.create(clusterSize);
		nodes = HashBiMap.create(clusterSize);
		
		for (ClusterNode clusterNode : NettyEmitter.this.topology.getTopology()
				.getNodes()) {
			Integer partition = clusterNode.getPartition();
			nodes.forcePut(partition, clusterNode);
		}

		ChannelFactory factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		bootstrap = new ClientBootstrap(factory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
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

	private void connectTo(Integer partitionId) {
		ClusterNode clusterNode = nodes.get(partitionId);

		if (clusterNode == null)
			logger.error("No ClusterNode exists for partitionId " + partitionId);

		logger.info(String.format("Connecting to %s:%d",
				clusterNode.getMachineName(), clusterNode.getPort()));
		while (true) {
			ChannelFuture f = this.bootstrap.connect(new InetSocketAddress(
					clusterNode.getMachineName(), clusterNode.getPort()));
			f.awaitUninterruptibly();
			if (f.isSuccess()) {
				channels.forcePut(partitionId, f.getChannel());
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ie) {
				logger.error(String.format(
						"Interrupted while connecting to %s:%d",
						clusterNode.getMachineName(), clusterNode.getPort()));
			}
		}
	}

	private Object sendLock = new Object();

	public void send(int partitionId, byte[] message) {
		Channel channel = channels.get(partitionId);

		while (channel == null) {
			connectTo(partitionId);
			channel = channels.get(partitionId); // making sure it is reflected in the map
		}

		ChannelBuffer buffer = ChannelBuffers.buffer(message.length);

		// check if Netty's send queue has gotten quite large
		if (!channel.isWritable()) {
			synchronized (sendLock) {
				// check again now that we have the lock
				while (!channel.isWritable()) {
					try {
						sendLock.wait(); // wait until the channel's queue
											// has gone down
					} catch (InterruptedException ie) {
						return; // somebody wants us to stop running
					}
				}
				// logger.info("Woke up from send block!");
			}
		}
		// between the above isWritable check and the below writeBytes, the
		// isWritable
		// may become false again. That's OK, we're just trying to avoid a
		// very large
		// above check to avoid creating a very large send queue inside
		// Netty.
		buffer.writeBytes(message);
		ChannelFuture f = channel.write(buffer);
		f.addListener(this);

	}

	public void operationComplete(ChannelFuture f) {
		// when we get here, the I/O operation associated with f is complete
		if (f.isCancelled()) {
			logger.error("Send I/O was cancelled!! "
					+ f.getChannel().getRemoteAddress());
		} else if (!f.isSuccess()) {
			logger.error("Exception on I/O operation", f.getCause());
			// find the partition associated with this broken channel
			int partition = channels.inverse().get(f.getChannel());
			logger.error(String
					.format("I/O on partition %d failed!", partition));
		}
	}

	public void onChange() {
		// do nothing for now, don't expect the topology to change.
	}

	public int getPartitionCount() {
		return topology.getTopology().getNodes().size();
	}

	class TestHandler extends SimpleChannelHandler {
		public void channelInterestChanged(ChannelHandlerContext ctx,
				ChannelStateEvent e) {
			// logger.info(String.format("%08x %08x %08x", e.getValue(),
			// e.getChannel().getInterestOps(), Channel.OP_WRITE));
			synchronized (sendLock) {
				if (e.getChannel().isWritable()) {
					sendLock.notify();
				}
			}
			ctx.sendUpstream(e);

		}

		public void exceptionCaught(ChannelHandlerContext context,
				ExceptionEvent event) {
			Integer partition = channels.inverse().get(context.getChannel());
			if (partition == null) {
				logger.error("Error on mystery channel!!");
				// return;
			}
			logger.error("Error on channel to partition " + partition);

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
