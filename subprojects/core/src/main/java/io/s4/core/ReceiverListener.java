package io.s4.core;

import io.s4.base.Event;


/**
 * 
 * Callback interface to pass {@link Event} objects received by a lower layer.
 * 
 */
public interface ReceiverListener {
    public void receiveEvent(Event event);
}
