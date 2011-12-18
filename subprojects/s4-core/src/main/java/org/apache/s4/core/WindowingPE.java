/*
 * Copyright (c) 2011 The S4 Project, http://s4.io.
 * All rights reserved.
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
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;
import org.apache.s4.base.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract ProcessingElement that can store historical values using a sliding window. Each set of values is called a
 * slot. The concrete class must implement a class (the slot class) where values are stored. Each slot represents a
 * segment of time or a fixed number of events. Slots are consecutive in time or events. The slot object cannot be null.
 * 
 * WHen using time-based slots, use this implementation only if you expect most slots to have values, it is not
 * efficient for sparse event streams.
 */
public abstract class WindowingPE<T> extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(WindowingPE.class);

    final private int numSlots;
    private CircularFifoBuffer<T> circularBuffer;
    final private Timer timer;
    final private long slotDurationInMilliseconds;

    /**
     * Constructor for time-based slots. The abstract method {@link #addPeriodicSlot()} is called periodically.
     * 
     * @param app
     *            the application
     * @param slotDuration
     *            the slot duration in timeUnit
     * @param timeUnit
     *            the unit of time
     * @param numSlots
     *            the number of slots to be stored
     */
    public WindowingPE(App app, long slotDuration, TimeUnit timeUnit, int numSlots) {
        super(app);
        this.numSlots = numSlots;

        if (slotDuration > 0l) {
            slotDurationInMilliseconds = TimeUnit.MILLISECONDS.convert(slotDuration, timeUnit);
            timer = new Timer();
            timer.schedule(new SlotTask(), slotDurationInMilliseconds, slotDurationInMilliseconds);
            logger.trace("TIMER: " + slotDurationInMilliseconds);

        } else {
            slotDurationInMilliseconds = 0;
            timer = null;
        }
    }

    /**
     * 
     * Constructor for the event-based slot. The abstract method {@link #addPeriodicSlot()} must be called by the
     * concrete class.
     * 
     * @param app
     *            the application
     * @param numSlots
     *            the number of slots to be stored
     */
    public WindowingPE(App app, int numSlots) {
        this(app, 0l, null, numSlots);
    }

    abstract protected void onEvent(Event event);

    abstract public void onTrigger(Event event);

    abstract public void onCreate();

    abstract public void onRemove();

    /**
     * This method is called at periodic intervals when a new slot must be put into the buffer. The concrete class must
     * implement the logic required to create a slot. For example, compute statistics from aggregations and get
     * variables ready for the new slot.
     * 
     * If the implementation class doesn't use periodic slots, this method will never be called. Use
     * {@link #addSlot(Object)} instead.
     * 
     * @return the slot object
     */
    abstract protected T addPeriodicSlot();

    /**
     * Add an object to the sliding window. Use it when the window is not periodic. For periodic slots use
     * {@link #addPeriodicSlot()} instead.
     * 
     * @param slot
     */
    protected void addSlot(T slot) {

        if (timer != null) {
            logger.error("Calling method addSlot() in a periodic window is not allowed.");
            return;
        }
        if (circularBuffer == null) {
            circularBuffer = new CircularFifoBuffer<T>(numSlots);
        }
        circularBuffer.add(slot);
    }

    /**
     * 
     * @return the least recently inserted slot
     */
    protected T getOldestSlot() {

        return circularBuffer.get();
    }

    /** Stops the the sliding window. */
    protected void stop() {
        timer.cancel();
    }

    /**
     * 
     * @return the collection of slots
     */
    protected Collection<T> getSlots() {

        return circularBuffer;
    }

    private class SlotTask extends TimerTask {

        @Override
        public void run() {

            logger.trace("START TIMER TASK");

            /* Iterate over all instances and put a new slot in the buffer. */
            for (Map.Entry<String, ProcessingElement> entry : getPEInstances().entrySet()) {
                logger.trace("pe id: " + entry.getValue().id);
                @SuppressWarnings("unchecked")
                WindowingPE<T> peInstance = (WindowingPE<T>) entry.getValue();

                if (peInstance.circularBuffer == null) {
                    peInstance.circularBuffer = new CircularFifoBuffer<T>(numSlots);
                }
                synchronized (peInstance) {
                    peInstance.circularBuffer.add(peInstance.addPeriodicSlot());
                }
            }
        }
    }
}
