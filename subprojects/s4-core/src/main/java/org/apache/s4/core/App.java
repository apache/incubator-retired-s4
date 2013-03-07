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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Sender;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.core.ft.CheckpointingFramework;
import org.apache.s4.core.staging.StreamExecutorServiceFactory;
import org.apache.s4.core.util.S4Metrics;
import org.apache.s4.core.window.AbstractSlidingWindowPE;
import org.apache.s4.core.window.SlotFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Container base class to hold all processing elements.
 * 
 * It is also where one defines the application graph: PE prototypes, internal streams, input and output streams.
 */
public abstract class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);

    /* All the PE prototypes in this app. */
    final private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();

    // /* Stream to PE prototype relations. */
    // final private Multimap<Streamable<? extends Event>, ProcessingElement> stream2pe = LinkedListMultimap.create();
    /* All the internal streams in this app. */
    final private List<Streamable<Event>> streams = new ArrayList<Streamable<Event>>();

    /* Pes indexed by name. */
    final Map<String, ProcessingElement> peByName = Maps.newHashMap();

    private ClockType clockType = ClockType.WALL_CLOCK;

    @Inject
    private Sender sender;
    @Inject
    private ReceiverImpl receiver;

    @Inject
    private Cluster cluster;

    @Inject
    private RemoteSenders remoteSenders;

    @Inject
    private Hasher hasher;

    @Inject
    private RemoteStreams remoteStreams;

    @Inject
    @Named("s4.cluster.name")
    private String clusterName;

    @Inject(optional = true)
    @Named("s4.metrics.peProcessingTime")
    boolean measurePEProcessingTime = true;

    // default is NoOpCheckpointingFramework
    @Inject
    CheckpointingFramework checkpointingFramework;

    @Inject
    S4Metrics metrics;

    // serialization uses the application class loader
    @Inject
    private SerializerDeserializerFactory serDeserFactory;
    private SerializerDeserializer serDeser;

    @Inject
    StreamExecutorServiceFactory streamExecutorFactory;

    @Inject
    private void initSerDeser() {
        this.serDeser = serDeserFactory.createSerializerDeserializer(getClass().getClassLoader());
    }

    /**
     * The internal clock can be configured as "wall clock" or "event clock". The wall clock computes time from the
     * system clock while the "event clock" uses the most recently seen event time stamp. TODO: implement event clock
     * functionality.
     */
    public enum ClockType {
        WALL_CLOCK, EVENT_CLOCK
    };

    /* Should only be used within the core package. */
    void addPEPrototype(ProcessingElement pePrototype) {
        pePrototypes.add(pePrototype);
    }

    public ProcessingElement getPE(String name) {

        return peByName.get(name);
    }

    /* Should only be used within the core package. */
    public void addStream(Streamable<Event> stream) {
        streams.add(stream);
    }

    /* Returns list of PE prototypes. Should only be used within the core package. */
    List<ProcessingElement> getPePrototypes() {
        return pePrototypes;
    }

    /* Returns list of internal streams. Should only be used within the core package. */
    // TODO visibility
    public List<Streamable<Event>> getStreams() {
        return streams;
    }

    protected abstract void onStart();

    /**
     * This method is called by the container after initialization. Once this method is called, threads get started and
     * events start flowing.
     */
    public final void start() {

        // logger.info("Prepare to start App [{}].", getClass().getName());
        //
        /* Start all streams. */
        for (Streamable<? extends Event> stream : getStreams()) {
            stream.start();
        }
        //
        // /* Allow abstract PE to initialize. */
        for (ProcessingElement pe : getPePrototypes()) {
            logger.info("Init prototype [{}].", pe.getClass().getName());
            pe.initPEPrototypeInternal();
        }

        onStart();
    }

    /**
     * This method is called by the container to initialize applications.
     */
    protected abstract void onInit();

    public final void init() {

        onInit();
    }

    /**
     * This method is called by the container before unloading the application.
     */
    protected abstract void onClose();

    public final void close() {

        onClose();
        removeAll();
    }

    private void removeAll() {

        /* Get the set of streams and close them. */
        for (Streamable<?> stream : getStreams()) {
            logger.trace("Closing stream [{}].", stream.getName());
            stream.close();
        }

        for (ProcessingElement pe : getPePrototypes()) {

            logger.trace("Removing PE proto [{}].", pe.getClass().getName());

            /* Remove all instances. */
            pe.removeAll();

        }

        /* Finally remove the entire app graph. */
        logger.trace("Clear app graph.");

        pePrototypes.clear();
        streams.clear();
    }

    void addPEPrototype(ProcessingElement pePrototype, Stream<? extends Event> stream) {

        // logger.info("Add PE prototype [{}] with stream [{}].", toString(pePrototype), toString(stream));
        pePrototypes.add(pePrototype);
        metrics.createCacheGauges(pePrototype, pePrototype.peInstances);

    }

    /**
     * The internal clock is configured as "wall clock" or "event clock" when this object is created.
     * 
     * @return the App time in milliseconds.
     */
    public long getTime() {
        return System.currentTimeMillis();
    }

    /**
     * The internal clock is configured as "wall clock" or "event clock" when this object is created.
     * 
     * @param timeUnit
     * @return the App time in timeUnit
     */
    public long getTime(TimeUnit timeUnit) {
        return timeUnit.convert(getTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * Set the {@link ClockType}.
     * 
     * @param clockType
     *            the clockTyoe for this app must be {@link ClockType#WALL_CLOCK} (default) or
     *            {@link ClockType#EVENT_CLOCK}
     */
    public void setClockType(ClockType clockType) {
        this.clockType = clockType;

        if (clockType == ClockType.EVENT_CLOCK) {
            logger.error("Event clock not implemented yet.");
            System.exit(1);
        }
    }

    /**
     * @return the clock type.
     */
    public ClockType getClockType() {
        return clockType;
    }

    /**
     * 
     * Returns the id of the partition assigned to the current node.
     * 
     * NOTE: This method will block until the current node gets assigned a partition
     * 
     */
    public int getPartitionId() {
        return getReceiver().getPartitionId();
    }

    /**
     * 
     * Returns the total number of partitions of the cluster this nodes belongs to.
     */
    public int getPartitionCount() {
        return cluster.getPhysicalCluster().getPartitionCount();
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
    public ReceiverImpl getReceiver() {
        return receiver;
    }

    public SerializerDeserializer getSerDeser() {
        return serDeser;
    }

    public CheckpointingFramework getCheckpointingFramework() {
        return checkpointingFramework;
    }

    public StreamExecutorServiceFactory getStreamExecutorFactory() {
        return streamExecutorFactory;
    }

    /**
     * Creates a stream with a specific key finder. The event is delivered to the PE instances in the target PE
     * prototypes by key.
     * 
     * <p>
     * If the value of the key is "joe" and the target PE prototypes are AddressPE and WorkPE, the event will be
     * delivered to the instances with key="joe" in the PE prototypes AddressPE and WorkPE.
     * 
     * @param name
     *            the name of the stream
     * @param finder
     *            the key finder object
     * @param eventType
     *            expected event type
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    protected <T extends Event> Stream<T> createStream(String name, KeyFinder<T> finder, Class<T> eventType,
            ProcessingElement... processingElements) {

        return new Stream<T>(this).setName(name).setKey(finder).setPEs(processingElements).setEventType(eventType)
                .setSerializerDeserializerFactory(serDeserFactory).register();
    }

    /**
     * @see App#createStream(String, KeyFinder, Class, ProcessingElement...)
     */
    protected <T extends Event> Stream<T> createStream(String name, KeyFinder<T> finder,
            ProcessingElement... processingElements) {
        return createStream(name, finder, null, processingElements);
    }

    /**
     * @see App#createStream(String, KeyFinder, Class, ProcessingElement...)
     */
    protected <T extends Event> Stream<T> createStream(String name, ProcessingElement... processingElements) {
        return createStream(name, null, processingElements);
    }

    /**
     * @see App#createStream(String, KeyFinder, Class, ProcessingElement...)
     */
    public <T extends Event> Stream<T> createStream(Class<T> type) {
        return createStream(null, null, type);
    }

    /**
     * Creates a "remote" stream, i.e. a stream that forwards events to remote clusters
     * 
     * @param name
     *            stream name, shared across communicating clusters
     * @param finder
     *            key finder
     * @return a reference to the created remote stream
     */
    protected <T extends Event> RemoteStream createOutputStream(String name, KeyFinder<Event> finder) {
        return new RemoteStream(this, name, finder, remoteSenders, hasher, remoteStreams, clusterName);
    }

    /**
     * @see App#createOutputStream(String, KeyFinder)
     */
    protected <T extends Event> RemoteStream createOutputStream(String name) {
        return createOutputStream(name, null);
    }

    /**
     * Creaters an "input" stream, i.e. a stream that listens to events from remote clusters, and that registers its
     * interest in the stream with the specified name.
     * 
     * @param streamName
     *            name of the remote stream
     * @param finder
     *            key finder
     * @param processingElements
     *            target processing elements
     * @return a reference to the created input stream
     */
    protected <T extends Event> Stream<T> createInputStream(String streamName, KeyFinder<T> finder,
            ProcessingElement... processingElements) {
        remoteStreams.addInputStream(clusterName, streamName);
        return createStream(streamName, finder, processingElements);

    }

    /**
     * @see App#createInputStream(String, KeyFinder, ProcessingElement...)
     */
    protected <T extends Event> Stream<T> createInputStream(String streamName, ProcessingElement... processingElements) {
        return createInputStream(streamName, null, processingElements);

    }

    /**
     * Creates a {@link ProcessingElement} prototype.
     * 
     * @param type
     *            the processing element type.
     * @param name
     *            a name for this PE prototype.
     * @return the processing element prototype.
     */
    public <T extends ProcessingElement> T createPE(Class<T> type, String name) {

        try {
            // TODO: make sure this doesn't crash if PE has a constructor other than with App as argument.
            Class<?>[] types = new Class<?>[] { App.class };
            try {
                T pe = type.getDeclaredConstructor(types).newInstance(this);
                pe.setName(name);
                return pe;
            } catch (NoSuchMethodException e) {
                // no such constructor. Use the setter
                T pe = type.getDeclaredConstructor(new Class[] {}).newInstance();
                pe.setApp(this);
                pe.setName(name);
                return pe;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Creates a {@link ProcessingElement} prototype.
     * 
     * @param type
     *            the processing element type.
     * @return the processing element prototype.
     */
    public <T extends ProcessingElement> T createPE(Class<T> type) {

        return createPE(type, null);

    }

    public <T extends AbstractSlidingWindowPE> T createSlidingWindowPE(Class<T> type, long slotDuration,
            TimeUnit timeUnit, int numSlots, SlotFactory slotFactory) {
        try {
            Class<?>[] types = new Class<?>[] { App.class, long.class, TimeUnit.class, int.class, SlotFactory.class };
            T pe = type.getDeclaredConstructor(types).newInstance(
                    new Object[] { this, slotDuration, timeUnit, numSlots, slotFactory });
            return pe;
        } catch (Exception e) {
            logger.error("Cannot instantiate pe for class [{}]", type.getName(), e);
            return null;
        }
    }

    static private String toString(ProcessingElement pe) {
        return pe != null ? pe.getClass().getName() + " " : "null ";
    }

    static private String toString(Streamable<? extends Event> stream) {
        return stream != null ? stream.getName() + " " : "null ";
    }

}
