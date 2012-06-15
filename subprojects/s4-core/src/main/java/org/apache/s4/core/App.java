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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.core.App.ClockType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Container base class to hold all processing elements. We will implement administrative methods here.
 */
public abstract class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);

    /* All the PE prototypes in this app. */
    final private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();

    // /* Stream to PE prototype relations. */
    // final private Multimap<Streamable<? extends Event>, ProcessingElement> stream2pe = LinkedListMultimap.create();
    /* All the internal streams in this app. */
    final private List<Streamable<Event>> streams = new ArrayList<Streamable<Event>>();

    /* All the the event sources exported by this app. */
    final private List<EventSource> eventSources = new ArrayList<EventSource>();

    /* Pes indexed by name. */
    Map<String, ProcessingElement> peByName = Maps.newHashMap();

    private ClockType clockType = ClockType.WALL_CLOCK;
    private int id = -1;

    @Inject
    private Sender sender;
    @Inject
    private Receiver receiver;

    @Inject
    RemoteSenders remoteSenders;

    @Inject
    Hasher hasher;

    @Inject
    RemoteStreams remoteStreams;

    @Inject
    @Named("cluster.name")
    String clusterName;

    // serialization uses the application class loader
    private SerializerDeserializer serDeser = new KryoSerDeser(getClass().getClassLoader());

    /**
     * The internal clock can be configured as "wall clock" or "event clock". The wall clock computes time from the
     * system clock while the "event clock" uses the most recently seen event time stamp. TODO: implement event clock
     * functionality.
     */
    public enum ClockType {
        WALL_CLOCK, EVENT_CLOCK
    };

    /**
     * @return true if the application is running in cluster mode.
     */
    // public boolean isCluster() {
    // return isCluster.booleanValue();
    // }

    /**
     * @return the unique app id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id
     *            the unique id for this app
     */
    public void setId(int id) {
        this.id = id;
    }

    /* Should only be used within the core package. */
    void addPEPrototype(ProcessingElement pePrototype) {
        pePrototypes.add(pePrototype);
    }

    public ProcessingElement getPE(String name) {

        return peByName.get(name);
    }

    /* Should only be used within the core package. */
    public void addStream(Streamable stream) {
        streams.add(stream);
    }

    /* Should only be used within the core package. */
    void addEventSource(EventSource es) {
        eventSources.add(es);
    }

    /* Returns list of PE prototypes. Should only be used within the core package. */
    List<ProcessingElement> getPePrototypes() {
        return pePrototypes;
    }

    // void addStream(Streamable<? extends Event> stream, ProcessingElement pePrototype) {
    // logger.info("Add Stream [{}] with PE prototype [{}].", toString(stream), toString(pePrototype));
    // stream2pe.put(stream, pePrototype);
    //
    // }

    /* Returns list of internal streams. Should only be used within the core package. */
    // TODO visibility
    public List<Streamable<Event>> getStreams() {
        return streams;
    }

    /* Returns list of the event sources to be exported. Should only be used within the core package. */
    // TODO visibility
    public List<EventSource> getEventSources() {
        return eventSources;
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
     *            the clockTyoe for this app must be {@link ClockType.WALL_CLOCK} (default) or
     *            {@link ClockType.EVENT_CLOCK}
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

    public SerializerDeserializer getSerDeser() {
        return serDeser;
    }

    /**
     * @param sender
     *            - sends events to the communication layer.
     * @param receiver
     *            - receives events from the communication layer.
     */
    public void setCommLayer(Sender sender, Receiver receiver) {
        // this.sender = sender;
        // this.receiver = receiver;
        // sender.setPartition(receiver.getPartition());
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
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    protected <T extends Event> Stream<T> createStream(String name, KeyFinder<T> finder,
            ProcessingElement... processingElements) {

        return new Stream<T>(this).setName(name).setKey(finder).setPEs(processingElements);
    }

    /**
     * Creates a broadcast stream that sends the events to all the PE instances in each of the target prototypes.
     * 
     * <p>
     * Keep in mind that if you had a million PE instances, the event would be delivered to all them.
     * 
     * @param name
     *            the name of the stream
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    protected <T extends Event> Stream<T> createStream(String name, ProcessingElement... processingElements) {

        return new Stream<T>(this).setName(name).setPEs(processingElements);
    }

    /**
     * Creates stream with default values. Use the builder methods to configure the stream. Example:
     * <p>
     * 
     * <pre>
     *  s1 = <SampleEvent> createStream().withName("My first stream.").withKey(new AKeyFinder()).to(somePE);
     * </pre>
     * 
     * <p>
     * 
     * @param name
     *            the name of the stream
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    public <T extends Event> Stream<T> createStream(Class<T> type) {

        Stream<T> stream = new Stream<T>(this);
        stream.setEventType(type);
        return stream;
    }

    protected <T extends Event> RemoteStream createOutputStream(String name) {
        return createOutputStream(name, null);
    }

    protected <T extends Event> RemoteStream createOutputStream(String name, KeyFinder<Event> finder) {
        return new RemoteStream(this, name, finder, remoteSenders, hasher, remoteStreams, clusterName);
    }

    protected <T extends Event> Stream<T> createInputStream(String streamName, KeyFinder<T> finder,
            ProcessingElement... processingElements) {
        remoteStreams.addInputStream(getId(), clusterName, streamName);
        return createStream(streamName, finder, processingElements);

    }

    protected <T extends Event> Stream<T> createInputStream(String streamName, ProcessingElement... processingElements) {
        remoteStreams.addInputStream(getId(), clusterName, streamName);
        return createStream(streamName, processingElements);

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

    public <T extends WindowingPE<?>> T createWindowingPE(Class<T> type, long slotDuration, TimeUnit timeUnit,
            int numSlots) {
        try {
            Class<?>[] types = new Class<?>[] { App.class, long.class, TimeUnit.class, int.class };
            T pe = type.getDeclaredConstructor(types).newInstance(
                    new Object[] { this, slotDuration, timeUnit, numSlots });
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
