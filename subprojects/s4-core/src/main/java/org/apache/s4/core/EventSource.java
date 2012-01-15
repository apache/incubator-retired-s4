package org.apache.s4.core;

import java.util.HashSet;
import java.util.Set;

import org.apache.s4.base.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * A producer app uses one or more EventSource classes to provide events to streamables. At runtime, consumer apps
 * subscribe to an event source by providing a streamable object. Each EventSource instance may correspond to a
 * different type of event stream. Each EventSource may have an unlimited number of subscribers.
 * 
 */
public class EventSource implements Streamable {

    /* No need to synchronize this object because we expect a single thread. */
    private Set<Streamable> streamables = new HashSet<Streamable>();
    private static final Logger logger = LoggerFactory.getLogger(EventSource.class);
    final private String name;

    public EventSource(App app, String name) {
        this.name = name;
        app.addEventSource(this);
    }

    /**
     * Subscribe a streamable to this event source.
     * 
     * @param aStream
     */
    public void subscribeStream(Streamable aStream) {
        logger.info("Subscribing stream: {} to event source: {}.", aStream.getName(), getName());
        streamables.add(aStream);
    }

    /**
     * Unsubscribe a streamable from this event source.
     * 
     * @param stream
     */
    public void unsubscribeStream(Streamable stream) {
        logger.info("Unsubsubscribing stream: {} to event source: {}.", stream.getName(), getName());
        streamables.remove(stream);
    }

    /**
     * Send an event to all the subscribed streamables.
     * 
     * @param event
     */
    @Override
    public void put(Event event) {
        for (Streamable stream : streamables) {
            stream.put(event);
        }
    }

    /**
     * 
     * @return the number of streamables subscribed to this event source.
     */
    public int getNumSubscribers() {
        return streamables.size();
    }

    /**
     * @return the name of this event source.
     */
    public String getName() {
        return name;
    }

    /**
     * Close all the streamables subscribed to this event source.
     */
    @Override
    public void close() {
        for (Streamable stream : streamables) {
            logger.info("Closing stream: {} in event source: {}.", stream.getName(), getName());
            stream.close();
        }
    }

    /**
     * 
     * @return the set of streamables subscribed to this event source.
     */
    public Set<Streamable> getStreamables() {
        return streamables;
    }
}
