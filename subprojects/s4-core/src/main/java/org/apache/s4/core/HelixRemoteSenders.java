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

package org.apache.s4.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.comm.topology.StreamConsumer;
import org.apache.s4.core.staging.RemoteSendersExecutorServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HelixRemoteSenders implements RemoteSenders {

    Logger logger = LoggerFactory.getLogger(HelixRemoteSenders.class);

    final RemoteEmitters remoteEmitters;

    final SerializerDeserializer serDeser;

    final Hasher hasher;

    ConcurrentMap<String, RemoteSender> sendersByTopology = new ConcurrentHashMap<String, RemoteSender>();

    private final ExecutorService executorService;

    private String clusterName;

    private RemoteSender sender;

    @Inject
    public HelixRemoteSenders(@Named("s4.cluster.name") String clusterName,Cluster topology,
            RemoteEmitters remoteEmitters,
            SerializerDeserializerFactory serDeserFactory, Hasher hasher,
            RemoteSendersExecutorServiceFactory senderExecutorFactory) {
        this.remoteEmitters = remoteEmitters;
        this.clusterName = clusterName;
        this.hasher = hasher;
        executorService = senderExecutorFactory.create();

        serDeser = serDeserFactory.createSerializerDeserializer(Thread
                .currentThread().getContextClassLoader());
        sender = new RemoteSender(topology,
                remoteEmitters.getEmitter(topology), hasher,
                clusterName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.RemoteSenders#send(java.lang.String,
     * org.apache.s4.base.Event)
     */
    @Override
    public void send(String hashKey, Event event) {
        event.setAppId(-1);
        // NOTE: this implies multiple serializations, there might be an
        // optimization
        executorService.execute(new SendToRemoteClusterTask(hashKey, event,
                sender));
    }

    class SendToRemoteClusterTask implements Runnable {

        String hashKey;
        Event event;
        RemoteSender sender;

        public SendToRemoteClusterTask(String hashKey, Event event,
                RemoteSender sender) {
            super();
            this.hashKey = hashKey;
            this.event = event;
            this.sender = sender;
        }

        @Override
        public void run() {
            try {
                sender.send(event.getStreamName(), hashKey,
                        serDeser.serialize(event));
            } catch (InterruptedException e) {
                logger.error(
                        "Interrupted blocking send operation for event {}. Event is lost.",
                        event);
                Thread.currentThread().interrupt();
            }

        }

    }
}
