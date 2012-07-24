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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.comm.topology.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class RemoteSenders {

    Logger logger = LoggerFactory.getLogger(RemoteSenders.class);

    @Inject
    RemoteEmitters emitters;

    @Inject
    RemoteStreams streams;

    @Inject
    Clusters topologies;

    @Inject
    SerializerDeserializer serDeser;

    @Inject
    Hasher hasher;

    Map<String, RemoteSender> sendersByTopology = new HashMap<String, RemoteSender>();

    public void send(String hashKey, Event event) {

        Set<StreamConsumer> consumers = streams.getConsumers(event.getStreamName());
        for (StreamConsumer consumer : consumers) {
            // NOTE: even though there might be several ephemeral znodes for the same app and topology, they are
            // represented by a single stream consumer
            RemoteSender sender = sendersByTopology.get(consumer.getClusterName());
            if (sender == null) {
                sender = new RemoteSender(emitters.getEmitter(topologies.getCluster(consumer.getClusterName())), hasher);
                // TODO cleanup when remote topologies die
                sendersByTopology.put(consumer.getClusterName(), sender);
            }
            // we must set the app id of the consumer app for correct dispatch within the consumer node
            // NOTE: this implies multiple serializations, there might be an optimization
            event.setAppId(consumer.getAppId());
            EventMessage eventMessage = new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(),
                    serDeser.serialize(event));
            sender.send(hashKey, eventMessage);
        }

    }
}
