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
package io.s4.core;


import io.s4.base.Event;
import io.s4.core.Receiver;
import io.s4.core.Sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * Container base class to hold all processing elements. We will implement administrative methods here. 
 */
public abstract class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    final private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();
    final private List<Streamable<? extends Event>> streams = new ArrayList<Streamable<? extends Event>>();
    private ClockType clockType = ClockType.WALL_CLOCK;
    private int id = -1;
    private Sender sender;
    private Receiver receiver;
    //@Inject private @Named("isCluster") Boolean isCluster;

    /**
     * The internal clock can be configured as "wall clock" or "event clock".
     * The wall clock computes time from the system clock while the
     * "event clock" uses the most recently seen event time stamp. TODO:
     * implement event clock functionality.
     */
    public enum ClockType {
        WALL_CLOCK, EVENT_CLOCK
    };

    /**
     * @return true if the application is running in cluster mode.
     */
//    public boolean isCluster() {
//        return isCluster.booleanValue();
//    }

    /**
     * @return the unique app id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the unique id for this app
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the pePrototypes
     */
    public List<ProcessingElement> getPePrototypes() {
        return pePrototypes;
    }

    protected abstract void start();

    protected abstract void init();

    protected abstract void close();

    public void removeAll() {

        for (ProcessingElement pe : pePrototypes) {

            /* Remove all instances. */
            pe.removeAll();

        }

        for (Streamable<? extends Event> stream : streams) {

            /* Close all streams. */
            stream.close();
        }

        /* Give prototype a chance to clean up after itself. */
        close();

        /* Finally remove from App. */
        pePrototypes.clear();
        streams.clear();
    }

    void addPEPrototype(ProcessingElement pePrototype) {

        pePrototypes.add(pePrototype);

    }

    void addStream(Streamable<? extends Event> stream) {

        streams.add(stream);

    }

    public List<Streamable<? extends Event>> getStreams() {
        return streams;
    }

    /**
     * The internal clock is configured as "wall clock" or "event clock" when
     * this object is created.
     * 
     * @return the App time in milliseconds.
     */
    public long getTime() {
        return System.currentTimeMillis();
    }

    /**
     * The internal clock is configured as "wall clock" or "event clock" when
     * this object is created.
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
     *            the clockTyoe for this app must be
     *            {@link ClockType.WALL_CLOCK} (default) or
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
     * @param sender the sender to set
     */
    public void setSender(Sender sender) {
        this.sender = sender;
    }

    /**
     * @param receiver the receiver to set
     */
    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    /**
     * Creates a stream with a specific key finder. The event is delivered to
     * the PE instances in the target PE prototypes by key.
     * 
     * <p>
     * If the value of the key is "joe" and the target PE prototypes are
     * AddressPE and WorkPE, the event will be delivered to the instances with
     * key="joe" in the PE prototypes AddressPE and WorkPE.
     * 
     * @param name
     *            the name of the stream
     * @param finder
     *            the key finder object
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    protected <T extends Event> Stream<T> createStream(String name,
            KeyFinder<T> finder, ProcessingElement... processingElements) {

        return new Stream<T>(this, name, finder, processingElements);
    }

    /**
     * Creates a broadcast stream that sends the events to all the PE instances
     * in each of the target prototypes.
     * 
     * <p>
     * Keep in mind that if you had a million PE instances, the event would be
     * delivered to all them.
     * 
     * @param name
     *            the name of the stream
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    protected <T extends Event> Stream<T> createStream(String name,
            ProcessingElement... processingElements) {

        return new Stream<T>(this, name, processingElements);
    }

    /**
     * Creates a {@link ProcessingElement} prototype.
     * 
     * @param type
     *            the processing element type.
     * @return the processing element prototype.
     */
    protected <T extends ProcessingElement> T createPE(Class<T> type) {

        try {
            // TODO: make sure this doesn't crash if PE has a constructor other than with App as argument.
            Class<?>[] types = new Class<?>[] { App.class };
            T pe = type.getDeclaredConstructor(types).newInstance(this);
            return pe;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}
