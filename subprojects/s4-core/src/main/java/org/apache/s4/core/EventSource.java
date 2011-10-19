package org.apache.s4.core;

import java.util.HashSet;
import java.util.Set;

import org.apache.s4.base.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * A producer app uses one or more EventSource classes to provide events to
 * streams. AT runtime, consumer apps subscribe to an event source by providing
 * a stream object. Each EventSource instance may correspond to a different type
 * of event stream. Each EventSource may have an unlimited number of
 * subscribers.
 * 
 */
public class EventSource<T extends Event> implements Streamable<T> {

    /* No need to synchronize this object because we expect a single thread. */
    private Set<Stream<T>> streams = new HashSet<Stream<T>>();
    private static final Logger logger = LoggerFactory
            .getLogger(EventSource.class);
    final private String name;

    public EventSource(App app, String name) {
        this.name = name;
        app.addStream(this);
    }

    /**
     * Subscribe a stream to this event source.
     * 
     * @param stream
     */
    public void subscribeStream(Stream<T> stream) {
        logger.info("Subscribing stream: {} to event source: {}.",
                stream.getName(), getName());
        streams.add(stream);
    }

    /**
     * Unsubscribe a stream from this event source.
     * 
     * @param stream
     */
    public void unsubscribeStream(Stream<T> stream) {
        logger.info("Unsubsubscribing stream: {} to event source: {}.",
                stream.getName(), getName());
        streams.remove(stream);
    }

    /**
     * Send an event to all the subscribed streams.
     * 
     * @param event
     */
    @Override
    public void put(T event) {
        for (Stream<T> stream : streams) {
            stream.put(event);
        }
    }

    /**
     * 
     * @return the number of streams subscribed to this event source.
     */
    public int getNumSubscribers() {
        return streams.size();
    }

    /**
     * @return the name of this event source
     */
    public String getName() {
        return name;
    }

    /**
     * Close all the streams subscribed to this event source.
     */
    @Override
    public void close() {
        for (Stream<T> stream : streams) {
            logger.info("Closing stream: {} in event source: {}.",
                    stream.getName(), getName());
            stream.close();
        }
    }
}
