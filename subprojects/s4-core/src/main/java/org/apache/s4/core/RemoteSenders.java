package org.apache.s4.core;

import org.apache.s4.base.Event;

/**
 * Sends events to remote clusters. Target clusters are selected dynamically based on the stream name information from
 * the event.
 * 
 */
public interface RemoteSenders {

    public abstract void send(String hashKey, Event event);

}
