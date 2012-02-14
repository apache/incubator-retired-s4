/*
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.core;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.s4.base.Event;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.Key;
import org.apache.s4.base.KeyFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * {@link Stream} and {@link ProcessingElement} objects represent the links and nodes in the application graph. A stream
 * sends an {@link Event} object to {@link ProcessingElement} instances located anywhere in a cluster.
 * <p>
 * Once a stream is instantiated, it is immutable.
 * <p>
 * To build an application create stream objects using use the {@link StreamFactory} class.
 */
public class Stream<T extends Event> extends Streamable<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Stream.class);

    final static private String DEFAULT_SEPARATOR = "^";
    final static private int CAPACITY = 1000;
    private static int idCounter = 0;
    private String name = "";
    private Key<T> key = null;
    private ProcessingElement[] targetPEs = null;
    final private BlockingQueue<T> queue = new ArrayBlockingQueue<T>(CAPACITY);
    private Thread thread;
    final private Sender sender;
    final private Receiver receiver;
    final private int id;
    final private App app;
    private Class<T> eventType = null;

    /**
     * Send events using a {@link KeyFinder<T>}. The key finder extracts the value of the key which is used to determine
     * the target {@link org.apache.s4.comm.topology.ClusterNode} for an event.
     * 
     * @param app
     *            we always register streams with the parent application.
     * @param name
     *            give this stream a meaningful name in the context of your application.
     * @param finder
     *            the finder object to find the value of the key in an event.
     * @param processingElements
     *            the target PE prototypes for this stream.
     */
    public Stream(App app) {
        synchronized (Stream.class) {
            id = idCounter++;
        }
        this.app = app;
        app.addStream(this, null);

        this.sender = app.getSender();
        this.receiver = app.getReceiver();
    }

    void start() {

        /* Get target PE prototypes for this stream. Remove null key. */
        Set<? extends ProcessingElement> pes = Sets.newHashSet(app.getTargetPEs(this));
        pes.remove(null);
        targetPEs = new ProcessingElement[pes.size()];
        pes.toArray(targetPEs);

        if (logger.isTraceEnabled()) {
            for (ProcessingElement pe : pes) {
                logger.trace("Starting stream [{}] with target PE [{}].", this.getName(), pe.getName());
            }
        }

        /* Start streaming. */
        thread = new Thread(this, name);
        thread.start();
        this.receiver.addStream(this);
    }

    /**
     * Stop and close this stream.
     */
    void close() {
        thread.interrupt();
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
        this.key = new Key<T>(keyFinder, DEFAULT_SEPARATOR);
        return this;
    }

    /**
     * Define the key finder for this stream using a descriptor.
     * 
     * @param keyFinderString
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
     * Send events from this stream to a PE.
     * 
     * @param pe
     *            a target PE.
     * 
     * @return the stream object
     */
    public Stream<T> setPE(ProcessingElement pe) {
        app.addStream(this, pe);
        return this;
    }

    /**
     * Send events from this stream to various PEs.
     * 
     * @param pe
     *            a target PE array.
     * 
     * @return the stream object
     */
    public Stream<T> setPEs(ProcessingElement[] pes) {
        for (int i = 0; i < pes.length; i++)
            app.addStream(this, pes[i]);
        return this;
    }

    /**
     * Sends an event.
     * 
     * @param event
     */
    public void put(T event) {
        try {
            event.setStreamId(getId());
            event.setAppId(app.getId());

            /*
             * Events may be sent to local or remote partitions or both. The following code implements the logic.
             */
            if (key != null) {

                /*
                 * We send to a specific PE instance using the key but we don't know if the target partition is remote
                 * or local. We need to ask the sender.
                 */
                if (sender.sendAndCheckIfLocal(key.get(event), event)) {

                    /*
                     * Sender checked and decided that the target is local so we simply put the event in the queue and
                     * we save the trip over the network.
                     */
                    queue.put(event);
                }

            } else {

                /*
                 * We are broadcasting this event to all PE instance. In a cluster, we need to send the event to every
                 * node. The sender method takes care of the remote partitions an we take care of putting the event into
                 * the queue.
                 */
                sender.sendToRemotePartitions(event);
                queue.put(event);
            }
        } catch (InterruptedException e) {
            if (logger.isTraceEnabled()) {
                e.printStackTrace();
            }
            logger.debug("Interrupted while waiting to put an event in the queue: {}.", e.getMessage());
            // System.exit(-1);
        }
    }

    /**
     * Implements the {@link ReceiverListener} interface. The low level {@link Receiver} object call this method when a
     * new {@link Event} is available.
     */
    @SuppressWarnings("unchecked")
    // Need casting because we don't know the concrete event type.
    public void receiveEvent(Event event) {
        try {
            queue.put((T) event);
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting to put an event in the queue: {}.", e.getMessage());
            // System.exit(-1);
        }
    }

    /**
     * @return the name
     */
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
     * @return the stream id
     */
    int getId() {
        return id;
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

    void setEventType(Class<T> type) {
        this.eventType = type;
    }

    @Override
    public void run() {
        while (true) {
            try {
                /* Get oldest event in queue. */
                T event = queue.take();

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
                        ProcessingElement pe;
                        pe = targetPEs[i].getInstanceForKey(key.get(event));

                        /* STEP 2: pass event to PE instance. */
                        pe.handleInputEvent(event);
                    }
                }

            } catch (InterruptedException e) {
                logger.info("Closing stream {}.", name);
                receiver.removeStream(this);
                return;
            }
        }
    }
}
