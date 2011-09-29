package io.s4.core;

import io.s4.comm.Receiver;
import io.s4.comm.Sender;

import com.google.inject.Inject;

/**
 * 
 * The Stream factory.
 * 
 */
public class StreamFactory {

    @Inject
    Sender sender;
    @Inject
    Receiver receiver;

    /**
     * Creates a stream with a specific key finder. The event is delivered to
     * the PE instances in the target PE prototypes by key.
     * 
     * <p>
     * If the value of the key is "joe" and the target PE prototypes are
     * AddressPE and WorkPE, the event will be delivered to the instances with
     * key="joe" in the PE prototypes AddressPE and WorkPE.
     * 
     * @param app
     *            the container app
     * @param name
     *            the name of the stream
     * @param finder
     *            the key finder object
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    public <T extends Event> Stream<T> create(App app, String name,
            KeyFinder<T> finder, ProcessingElement... processingElements) {

        return new Stream<T>(app, name, finder, sender, receiver,
                processingElements);
    }

    /**
     * Creates a broadcast stream that sends the events to all the PE instances
     * in each of the target prototypes.
     * 
     * <p>
     * Keep in mind that if you had a million PE instances, the event would be
     * delivered to all them.
     * 
     * @param app
     *            the container app
     * @param name
     *            the name of the stream
     * @param processingElements
     *            the target processing elements
     * @return the stream
     */
    public <T extends Event> Stream<T> create(App app, String name,
            ProcessingElement... processingElements) {

        return new Stream<T>(app, name, sender, receiver, processingElements);
    }
}
