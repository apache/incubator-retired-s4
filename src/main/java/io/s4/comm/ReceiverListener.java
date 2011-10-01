package io.s4.comm;

import io.s4.core.Event;

/**
 * 
 * Callback interface to pass {@link Event} objects received by a lower layer.
 * 
 */
public interface ReceiverListener {
    public void receiveEvent(Event event);
}
