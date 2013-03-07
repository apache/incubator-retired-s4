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

import java.util.Collection;
import java.util.concurrent.Executor;

import org.apache.s4.base.Event;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.Key;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Receiver;
import org.apache.s4.base.Sender;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * {@link Stream} and {@link ProcessingElement} objects represent the links and nodes in the application graph. A stream
 * sends an {@link Event} object to {@link ProcessingElement} instances located anywhere in a cluster.
 * <p>
 * Once a stream is instantiated, it is immutable.
 * <p>
 * To build an application, create stream objects using relevant methods in the {@link App} class.
 */
public class Stream<T extends Event> implements Streamable {

    private static final Logger logger = LoggerFactory.getLogger(Stream.class);

    final static private String DEFAULT_SEPARATOR = "^";
    private String name;
    protected Key<T> key;
    private ProcessingElement[] targetPEs;
    private Executor eventProcessingExecutor;
    final private Sender sender;
    final private ReceiverImpl receiver;
    // final private int id;
    final private App app;
    private Class<T> eventType = null;
    SerializerDeserializer serDeser;

    private int parallelism = 1;

    /**
     * Send events using a {@link KeyFinder}. The key finder extracts the value of the key which is used to determine
     * the target {@link org.apache.s4.comm.topology.ClusterNode} for an event.
     * 
     * @param app
     *            we always register streams with the parent application.
     */
    public Stream(App app) {
        this.app = app;
        this.sender = app.getSender();
        this.receiver = app.getReceiver();
    }

    @Override
    public void start() {
        app.metrics.createStreamMeters(getName());
        if (logger.isTraceEnabled()) {
            if (targetPEs != null) {
                for (ProcessingElement pe : targetPEs) {
                    logger.trace("Starting stream [{}] with target PE [{}].", this.getName(), pe.getName());
                }
            }
        }

        eventProcessingExecutor = app.getStreamExecutorFactory().create(parallelism, name,
                app.getClass().getClassLoader());

        this.receiver.addStream(this);
    }

    /**
     * Name the stream.
     * 
     * @param name
     *            the stream name, default is an empty string.
     * @return the stream object
     */
    public Stream<T> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Define the key finder for this stream.
     * 
     * @param keyFinder
     *            a function to lookup the value of the key.
     * @return the stream object
     */
    public Stream<T> setKey(KeyFinder<T> keyFinder) {
        if (keyFinder == null) {
            this.key = null;
        } else {
            this.key = new Key<T>(keyFinder, DEFAULT_SEPARATOR);
        }
        return this;
    }

    Stream<T> setEventType(Class<T> type) {
        this.eventType = type;
        return this;
    }

    /**
     * Define the key finder for this stream using a descriptor.
     * 
     * @param keyName
     *            a descriptor to lookup up the value of the key.
     * @return the stream object
     */
    public Stream<T> setKey(String keyName) {

        Preconditions.checkNotNull(eventType);

        KeyFinder<T> kf = new GenericKeyFinder<T>(keyName, eventType);
        setKey(kf);

        return this;
    }

    /**
     * Send events from this stream to one or more PEs.
     * 
     * @param pes
     *            one or more target prototypes
     * 
     * 
     * @return the stream object
     */
    public Stream<T> setPEs(ProcessingElement... pes) {
        this.targetPEs = pes;
        return this;
    }

    /**
     * Sends an event.
     * 
     * @param event
     */
    @Override
    @SuppressWarnings("unchecked")
    public void put(Event event) {
        event.setStreamId(getName());

        /*
         * Events may be sent to local or remote partitions or both. The following code implements the logic.
         */
        if (key != null) {

            /*
             * We send to a specific PE instance using the key but we don't know if the target partition is remote or
             * local. We need to ask the sender.
             */
            if (!sender.checkAndSendIfNotLocal(key.get((T) event), event)) {

                /*
                 * Sender checked and decided that the target is local so we simply put the event in the queue and we
                 * save the trip over the network.
                 */
                eventProcessingExecutor.execute(new StreamEventProcessingTask((T) event));
            }

        } else {

            /*
             * We are broadcasting this event to all PE instance. In a cluster, we need to send the event to every node.
             * The sender method takes care of the remote partitions an we take care of putting the event into the
             * queue.
             */
            sender.sendToAllRemotePartitions(event);

            // now send to local queue
            eventProcessingExecutor.execute(new StreamEventProcessingTask((T) event));
            // TODO abstraction around queue and add dropped counter
            // TODO add counter for local events

        }
    }

    /**
     * The low level {@link ReceiverImpl} object call this method when a new {@link Event} is available.
     */
    public void receiveEvent(Event event) {
        // NOTE: ArrayBlockingQueue.size is O(1).

        eventProcessingExecutor.execute(new StreamEventProcessingTask((T) event));
        // TODO abstraction around queue and add dropped counter
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @return the key
     */
    public Key<T> getKey() {
        return key;
    }

    /**
     * @return the app
     */
    public App getApp() {
        return app;
    }

    /**
     * @return the list of target processing element prototypes.
     */
    public ProcessingElement[] getTargetPEs() {
        return targetPEs;
    }

    /**
     * Stop and close this stream.
     */
    @Override
    public void close() {
    }

    /**
     * @return the sender object
     */
    public Sender getSender() {
        return sender;
    }

    /**
     * @return the receiver object
     */
    public Receiver getReceiver() {
        return receiver;
    }

    public Stream<T> register() {
        app.addStream(this);
        return this;
    }

    public Stream<T> setSerializerDeserializerFactory(SerializerDeserializerFactory serDeserFactory) {
        this.serDeser = serDeserFactory.createSerializerDeserializer(getClass().getClassLoader());
        return this;
    }

    /**
     * <p>
     * Defines the maximum number of concurrent threads that should be used for processing events for this stream.
     * Threads will only be created as necessary, up to the specified maximum.
     * </p>
     * <p>
     * Default is 1 (i.e. with default stream executor service, this corresponds to asynchronous processing, but no
     * parallelism)
     * </p>
     * <p>
     * It might be useful to increase parallelism when:
     * <ul>
     * <li>Processing elements handling events for this stream are CPU bound</li>
     * <li>Processing elements handling events for this stream use blocking I/O operations</li>
     * </ul>
     * <p>
     * 
     * 
     */
    public Stream<T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    class StreamEventProcessingTask implements Runnable {

        T event;

        public StreamEventProcessingTask(T event) {
            this.event = event;
        }

        @Override
        public void run() {
            app.metrics.dequeuedEvent(name);

            /* Send event to each target PE. */
            for (int i = 0; i < targetPEs.length; i++) {

                if (key == null) {

                    /* Broadcast to all PE instances! */

                    /* STEP 1: find all PE instances. */

                    Collection<ProcessingElement> pes = targetPEs[i].getInstances();

                    /* STEP 2: iterate and pass event to PE instance. */
                    for (ProcessingElement pe : pes) {

                        pe.handleInputEvent(event);
                    }

                } else {

                    /* We have a key, send to target PE. */

                    /* STEP 1: find the PE instance for key. */
                    ProcessingElement pe = targetPEs[i].getInstanceForKey(key.get(event));

                    /* STEP 2: pass event to PE instance. */
                    pe.handleInputEvent(event);
                }
            }

        }

    }
}
