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

package org.apache.s4.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * <p>
 * Fetches modules and app archives through HTTP.
 * </p>
 * <p>
 * The underlying implementation uses Netty, and borrows code from the Netty snoop example.</br>
 * 
 * @see <a href="http://docs.jboss.org/netty/3.2/xref/org/jboss/netty/example/http/snoop/package-summary.html">Netty
 *      snoop example</a>
 * 
 *      </p>
 */
public class HttpArchiveFetcher implements ArchiveFetcher {

    private static Logger logger = LoggerFactory.getLogger(HttpArchiveFetcher.class);

    @Override
    public InputStream fetch(URI uri) throws ArchiveFetchException {
        logger.debug("Fetching file through http: {}", uri.toString());

        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            if (uri.getScheme().equalsIgnoreCase("http")) {
                port = 80;
            } else if (uri.getScheme().equalsIgnoreCase("https")) {
                port = 443;
            }
        }

        ClientBootstrap clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        File tmpFile;
        try {
            tmpFile = File.createTempFile("http", "download");
        } catch (IOException e) {
            throw new ArchiveFetchException("Cannot create temporary file for fetching archive data from http server",
                    e);
        }
        clientBootstrap.setPipelineFactory(new HttpClientPipelineFactory(tmpFile));
        ChannelFuture channelFuture = clientBootstrap.connect(new InetSocketAddress(host, port));
        // TODO timeout?
        Channel channel = channelFuture.awaitUninterruptibly().getChannel();
        if (!channelFuture.isSuccess()) {
            clientBootstrap.releaseExternalResources();
            throw new ArchiveFetchException("Cannot connect to http uri [" + uri.toString() + "]",
                    channelFuture.getCause());
        }

        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getPath());
        request.setHeader(HttpHeaders.Names.HOST, host);
        request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

        channel.write(request);

        channel.getCloseFuture().awaitUninterruptibly();

        clientBootstrap.releaseExternalResources();

        logger.debug("Finished downloading archive file through http {}, as file: {}", uri.toString(),
                tmpFile.getAbsolutePath());
        try {
            return new FileInputStream(tmpFile);
        } catch (FileNotFoundException e) {
            throw new ArchiveFetchException("Cannot get input stream from temporary file with s4r data ["
                    + tmpFile.getAbsolutePath() + "]");
        }
    }

    private class HttpClientPipelineFactory implements ChannelPipelineFactory {

        File tmpFile;

        public HttpClientPipelineFactory(File tmpFile) {
            this.tmpFile = tmpFile;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            // Create a default pipeline implementation.
            ChannelPipeline pipeline = Channels.pipeline();

            pipeline.addLast("codec", new HttpClientCodec());

            // Remove the following line if you don't want automatic content decompression.
            pipeline.addLast("inflater", new HttpContentDecompressor());

            pipeline.addLast("handler", new HttpResponseHandler(tmpFile));
            return pipeline;
        }
    }

    // see http://docs.jboss.org/netty/3.2/xref/org/jboss/netty/example/http/snoop/HttpResponseHandler.html
    private class HttpResponseHandler extends SimpleChannelUpstreamHandler {

        private boolean readingChunks;
        FileOutputStream fos;

        public HttpResponseHandler(File tmpFile) throws FileNotFoundException {
            this.fos = new FileOutputStream(tmpFile);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            if (!readingChunks) {
                HttpResponse response = (HttpResponse) e.getMessage();

                if (response.isChunked()) {
                    readingChunks = true;
                } else {
                    copyContentToTmpFile(response.getContent());
                }
            } else {
                HttpChunk chunk = (HttpChunk) e.getMessage();
                if (chunk.isLast()) {
                    readingChunks = false;
                    fos.close();
                } else {
                    copyContentToTmpFile(chunk.getContent());
                }
            }

        }

        private void copyContentToTmpFile(ChannelBuffer content) throws IOException, FileNotFoundException {
            ChannelBufferInputStream cbis = new ChannelBufferInputStream(content);
            ByteStreams.copy(cbis, fos);
        }
    }
}
