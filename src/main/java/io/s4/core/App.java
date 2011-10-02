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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/*
 * Container base class to hold all processing elements. We will implement administrative methods here. 
 */
public abstract class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    final private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();
    final private List<Stream<? extends Event>> streams = new ArrayList<Stream<? extends Event>>();
    final private ClockType clockType;
    @Inject protected StreamFactory streamFactory;

    /**
     * The internal clock can be configured as "wall clock" or "event clock".
     * The wall clock computes time from the system clock while the
     * "event clock" uses the most recently seen event time stamp. TODO:
     * implement event clock functionality.
     */
    public enum ClockType {
        WALL_CLOCK, EVENT_CLOCK
    };

    /** The no args constructor sets the clock type to "Wall Clock". */
    protected App() {
        this.clockType = ClockType.WALL_CLOCK;
    }

    /** Explicitly sets the clock type. */
    protected App(ClockType clockType) {
        this.clockType = clockType;

        if (clockType == ClockType.EVENT_CLOCK) {
            logger.error("Event clock not implemented yet.");
            System.exit(1);
        }
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

        for (Stream<? extends Event> stream : streams) {

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

    void addStream(Stream<? extends Event> stream) {

        streams.add(stream);

    }

    public List<Stream<? extends Event>> getStreams() {
        return streams;
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
     * @return the clock type.
     */
    public ClockType getClockType() {
        return clockType;
    }
}
