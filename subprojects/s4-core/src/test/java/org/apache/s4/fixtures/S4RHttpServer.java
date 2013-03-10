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
package org.apache.s4.fixtures;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.handler.codec.http.HttpHeaders;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class S4RHttpServer {

    int port;
    File dir;
    private HttpServer httpServer;

    public S4RHttpServer(int port, File dir) {
        this.port = port;
        this.dir = dir;
    }

    public void start() throws IOException {
        InetSocketAddress addr = new InetSocketAddress(8080);
        httpServer = HttpServer.create(addr, 0);

        httpServer.createContext("/s4", new HttpS4RServingHandler(dir));
        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    class HttpS4RServingHandler implements HttpHandler {

        File tmpDir;

        public HttpS4RServingHandler(File tmpDir) {
            this.tmpDir = tmpDir;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String requestMethod = exchange.getRequestMethod();
            if (requestMethod.equalsIgnoreCase("GET")) {
                String fileName = exchange.getRequestURI().getPath().substring("/s4/".length());
                Headers responseHeaders = exchange.getResponseHeaders();
                responseHeaders.set(HttpHeaders.Names.CONTENT_TYPE, HttpHeaders.Values.BYTES);
                exchange.sendResponseHeaders(200, Files.toByteArray(new File(tmpDir, fileName)).length);

                OutputStream responseBody = exchange.getResponseBody();

                ByteStreams.copy(new FileInputStream(new File(tmpDir, fileName)), responseBody);

                responseBody.close();
            }
        }
    }

}
