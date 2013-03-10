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
package org.apache.s4;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.s4.base.Event;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.ZkClient;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class TestListener implements Listener {

    private static Logger logger = LoggerFactory.getLogger(TestListener.class);

    private ServerBootstrap bootstrap;
    private final ChannelGroup channels = new DefaultChannelGroup();
    private ZkClient zkClient;
    private SerializerDeserializer serDeser;

    @Inject
    public TestListener(Assignment assignment, @Named("s4.comm.timeout") int timeout, SerializerDeserializer serDeser) {
        // wait for an assignment
        ClusterNode node = assignment.assignClusterNode();
        this.serDeser = serDeser;

        ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                ChannelPipeline p = Channels.pipeline();
                p.addLast("1", new LengthFieldBasedFrameDecoder(999999, 0, 4, 0, 4));
                p.addLast("2", new MyChannelHandler());

                return p;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", timeout);
        bootstrap.setOption("readWriteFair", true);

        Channel c = bootstrap.bind(new InetSocketAddress(node.getPort()));
        channels.add(c);

        zkClient = new ZkClient("localhost:2181");
    }

    @Override
    public void close() {
        try {
            channels.close().await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        bootstrap.releaseExternalResources();

    }

    @Override
    public int getPartitionId() {
        throw new NotImplementedException();
    }

    public class MyChannelHandler extends SimpleChannelHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            channels.add(e.getChannel());
            ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
            Event event = (Event) serDeser.deserialize(buffer.toByteBuffer());

            zkClient.createEphemeralSequential("/test/data", event.get("message", long.class));

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) {
            logger.error("Error", event.getCause());
            Channel c = context.getChannel();
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
    }

}
