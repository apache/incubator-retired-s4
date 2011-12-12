package org.apache.s4.core;

import org.apache.s4.base.Event;

/**
 * We use this interface to put events into objects.
 * 
 * @param <T>
 */
abstract class Streamable<T extends Event> {

    /**
     * Put an event into the streams.
     * 
     * @param event
     */
    abstract void put(T event);

    /**
     * Stop and close all the streams.
     */
    abstract void close();

    /**
     * @return the name of this streamable object.
     */
    abstract String getName();

    /**
     * Start all streams;
     */
    abstract void start();
}
