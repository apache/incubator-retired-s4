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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

/*
 * Container base class to hold all processing elements. We will implement administrative methods here. 
 */
public abstract class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);

    /* PE prototype to streams relations. */
    final private Multimap<ProcessingElement, Streamable<? extends Event>> pe2stream = LinkedListMultimap.create();

    /* Stream to PE prototype relations. */
    final private Multimap<Streamable<? extends Event>, ProcessingElement> stream2pe = LinkedListMultimap.create();

    /* Pes indexed by name. */
    Map<String, ProcessingElement> peByName = Maps.newHashMap();

    private ClockType clockType = ClockType.WALL_CLOCK;
    private int id = -1;
    @Inject
    private Sender sender;
    @Inject
    private Receiver receiver;

    // @Inject private @Named("isCluster") Boolean isCluster;

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

    /**
     * @return all the pePrototypes
     */
    Collection<ProcessingElement> getPePrototypes() {
        return pe2stream.keySet();
    }

    /**
     * @return all the pePrototypes
     */
    <T extends Event> Collection<ProcessingElement> getTargetPEs(Stream<T> stream) {

        Map<Streamable<?>, Collection<ProcessingElement>> stream2peMap = stream2pe.asMap();

        return stream2peMap.get(stream);
    }

    protected abstract void onStart();

    /**
     * This method is called by the container after initialization. Once this method is called, threads get started and
     * events start flowing.
     */
    protected void start() {

        logger.info("Prepare to start App [{}].", getClass().getName());

        /* Start all streams. */
        for (Streamable<? extends Event> stream : getStreams()) {
            stream.start();
        }

        /* Allow abstract PE to initialize. */
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

    protected void init() {

        onInit();
    }

    /**
     * This method is called by the container before unloading the application.
     */
    protected abstract void onClose();

    protected void close() {

        onClose();
        removeAll();
    }

    private void removeAll() {

        for (ProcessingElement pe : getPePrototypes()) {

            logger.trace("Removing PE proto [{}].", pe.getClass().getName());

            /* Remove all instances. */
            pe.removeAll();

        }

        /* Get the set of streams and close them. */
        for (Streamable<?> stream : getStreams()) {
            logger.trace("Closing stream [{}].", stream.getName());
            stream.close();
        }

        /* Finally remove the entire app graph. */
        logger.trace("Clear app graph.");
        pe2stream.clear();
        stream2pe.clear();
    }

    void addPEPrototype(ProcessingElement pePrototype, Stream<? extends Event> stream) {

        logger.info("Add PE prototype [{}] with stream [{}].", toString(pePrototype), toString(stream));
        pe2stream.put(pePrototype, stream);
    }

    public ProcessingElement getPE(String name) {

        return peByName.get(name);
    }

    void addStream(Streamable<? extends Event> stream, ProcessingElement pePrototype) {
        logger.info("Add Stream [{}] with PE prototype [{}].", toString(stream), toString(pePrototype));
        stream2pe.put(stream, pePrototype);

    }

    Collection<Streamable<? extends Event>> getStreams() {
        return stream2pe.keySet();
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

    /**
     * @param sender
     *            - sends events to the communication layer.
     * @param receiver
     *            - receives events from the communication layer.
     */
    public void setCommLayer(Sender sender, Receiver receiver) {
        this.sender = sender;
        this.receiver = receiver;
        sender.setPartition(receiver.getPartition());
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
            T pe = type.getDeclaredConstructor(types).newInstance(this);
            pe.setName(name);
            return pe;

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

    static private String toString(ProcessingElement pe) {
        return pe != null ? pe.getClass().getName() + " " : "null ";
    }

    static private String toString(Streamable<? extends Event> stream) {
        return stream != null ? stream.getName() + " " : "null ";
    }

    /**
     * Facility for starting S4 apps by passing a module class and an application class
     * 
     * Usage: java &ltclasspath+params&gt org.apache.s4.core.App &ltappClassName&gt &ltmoduleClassName&gt
     * 
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            usage(args);
        }
        logger.info("Starting S4 app with module [{}] and app [{}]", args[0], args[1]);
        Injector injector = null;
        try {
            if (!AbstractModule.class.isAssignableFrom(Class.forName(args[0]))) {
                logger.error("Module class [{}] is not an instance of [{}]", args[0], AbstractModule.class.getName());
                System.exit(-1);
            }
            injector = Guice.createInjector((AbstractModule) Class.forName(args[0]).newInstance());
        } catch (InstantiationException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        } catch (IllegalAccessException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        }
        App app;
        try {
            app = (App) injector.getInstance(Class.forName(args[1]));
            app.init();
            app.start();
        } catch (ClassNotFoundException e) {
            logger.error("Invalid S4 application class [{}] : {}", args[0], e.getMessage());
        }
    }

    private static void usage(String[] args) {
        logger.info("Invalid parameters " + Arrays.toString(args)
                + " \nUsage: java <classpath+params> org.apache.s4.core.App <appClassName> <moduleClassName>");
        System.exit(-1);
    }

}
