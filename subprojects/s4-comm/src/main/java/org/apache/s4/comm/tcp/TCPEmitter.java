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
import org.apache.helix.model.InstanceConfig;

/**
 * TCPEmitter - Uses TCP to send messages across partitions.
 * 
 */

public class TCPEmitter implements Emitter, ClusterChangeListener {
	private static final Logger logger = LoggerFactory
			.getLogger(TCPEmitter.class);

	private final int nettyTimeout;

	private Cluster topology;
	private final ClientBootstrap bootstrap;

	/*
	 * All channels
	 */
	private final ChannelGroup channels = new DefaultChannelGroup();

	/*
	 * Channel used to send messages to each Node
	 */
	private final BiMap<InstanceConfig, Channel> nodeChannelMap;

	// lock for synchronizing between cluster updates callbacks and other code
	private final Lock lock;

	@Inject
	SerializerDeserializer serDeser;

	@Inject
	public TCPEmitter(Cluster topology, @Named("s4.comm.timeout") int timeout)
			throws InterruptedException {
		this.nettyTimeout = timeout;
		this.topology = topology;
		this.lock = new ReentrantLock();

		// Initialize data structures
		//int clusterSize = this.topology.getPhysicalCluster().getNodes().size();
		// TODO cluster can grow in size
		nodeChannelMap = Maps.synchronizedBiMap(HashBiMap
				.<InstanceConfig, Channel> create());

		// Initialize netty related structures
		ChannelFactory factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
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
	}

	private boolean connectTo(InstanceConfig config) {

		if (config == null) {

			logger.error("Invalid clusterNode");
			return false;
		}

		try {
			ChannelFuture connectFuture = this.bootstrap
					.connect(new InetSocketAddress(config.getHostName(),
							Integer.parseInt(config.getPort())));
			connectFuture.await();
			if (connectFuture.isSuccess()) {
				channels.add(connectFuture.getChannel());
				nodeChannelMap
						.forcePut(config, connectFuture.getChannel());
				return true;
			}
		} catch (InterruptedException ie) {
			logger.error(String.format("Interrupted while connecting to %s:%d",
					config.getHostName(), config.getPort()));
			Thread.currentThread().interrupt();
		}
		return false;
	}

	private void sendMessage(String streamName, int partitionId, byte[] message) {
		ChannelBuffer buffer = ChannelBuffers.buffer(message.length);
		buffer.writeBytes(message);
		InstanceConfig config = topology
				.getDestination(streamName, partitionId);
		if (!nodeChannelMap.containsKey(config)) {
			if (!connectTo(config)) {
				// Couldn't connect, discard message
				return;
			}
		}

		Channel c = nodeChannelMap.get(partitionId);
		if (c == null)
			return;

		c.write(buffer).addListener(new MessageSendingListener(partitionId));
	}

	@Override
	public boolean send(int partitionId, EventMessage message) {
		sendMessage(message.getStreamName(), partitionId,
				serDeser.serialize(message));
		return true;
	}

	protected void removeChannel(int partition) {
		Channel c = nodeChannelMap.remove(partition);
		if (c == null) {
			return;
		}
		c.close().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future)
					throws Exception {
				if (future.isSuccess())
					channels.remove(future.getChannel());
				else
					logger.error("Failed to close channel");
			}
		});
	}

	public void close() {
		try {
			channels.close().await();
			bootstrap.releaseExternalResources();
		} catch (InterruptedException ie) {
			logger.error("Interrupted while closing");
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public int getPartitionCount() {
		return topology.getPhysicalCluster().getPartitionCount();
	}

	class ExceptionHandler extends SimpleChannelUpstreamHandler {
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			Throwable t = e.getCause();
			if (t instanceof ClosedChannelException) {
				nodeChannelMap.inverse().remove(e.getChannel());
				return;
			} else if (t instanceof ConnectException) {
				nodeChannelMap.inverse().remove(e.getChannel());
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
			if (!future.isSuccess()) {
				try {
					// TODO handle possible cluster reconfiguration between send
					// and failure callback
					logger.warn(
							"Failed to send message to node {} (according to current cluster information)",
							topology.getPhysicalCluster().getNodes()
									.get(partitionId));
				} catch (IndexOutOfBoundsException ignored) {
					// cluster was changed
				}
			}

		}
	}

	@Override
	public void onChange() {
		// TODO Auto-generated method stub
		
	}
}
