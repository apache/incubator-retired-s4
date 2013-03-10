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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.s4.base.Event;
import org.apache.s4.base.Listener;
import org.apache.s4.base.Receiver;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.core.util.S4Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * The {@link ReceiverImpl} and its counterpart {@link SenderImpl} are the top level classes of the communication layer.
 * <p>
 * {@link ReceiverImpl} is responsible for receiving an event to a {@link ProcessingElement} instance using a hashKey.
 * <p>
 * A Listener implementation receives data from the network and passes an event as a byte array to the
 * {@link ReceiverImpl}. The byte array is deserialized and converted into an {@link Event}. Finally the event is passed
 * to the matching streams.
 * </p>
 * There is a single {@link ReceiverImpl} instance per node.
 * 
 * Details on how the cluster is partitioned and how events are serialized and transmitted to its destination are hidden
 * from the application developer. </p>
 */
@Singleton
public class ReceiverImpl implements Receiver {

    private static final Logger logger = LoggerFactory.getLogger(ReceiverImpl.class);

    final private Listener listener;
    final private SerializerDeserializer serDeser;
    private final Map<String, Stream<? extends Event>> streams;

    @Inject
    S4Metrics metrics;

    @Inject
    public ReceiverImpl(Listener listener, SerializerDeserializer serDeser) {
        this.listener = listener;
        this.serDeser = serDeser;

        streams = new MapMaker().makeMap();
    }

    @Override
    public int getPartitionId() {
        return listener.getPartitionId();
    }

    /** Save stream keyed by app id and stream id. */
    void addStream(Stream<? extends Event> stream) {
        streams.put(stream.getName(), stream);
    }

    /** Remove stream when it is no longer needed. */
    void removeStream(Stream<? extends Event> stream) {
        streams.remove(stream.getName());
    }

    @Override
    public void receive(ByteBuffer message) {
        metrics.receivedEventFromCommLayer(message.array().length);
        Event event = (Event) serDeser.deserialize(message);

        String streamId = event.getStreamId();

        /*
         * Match streamId in event to the target stream and pass the event to the target stream. TODO: make this more
         * efficient for the case in which we send the same event to multiple PEs.
         */
        try {
            streams.get(streamId).receiveEvent(event);
        } catch (NullPointerException e) {
            logger.error("Could not find target stream for event with streamId={}", streamId);
        }
    }

}
