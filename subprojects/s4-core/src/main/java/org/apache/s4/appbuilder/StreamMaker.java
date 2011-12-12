package org.apache.s4.appbuilder;

import org.apache.s4.base.Event;
import org.apache.s4.core.KeyFinder;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class StreamMaker {

    private String name = "";
    private KeyFinder<?> keyFinder = null;
    private String keyFinderString;
    private PEMaker pem;
    private Class<? extends Event> type;
    private AppMaker appMaker;

    /* Only package classes can instantiate this class. */
    StreamMaker(AppMaker appMaker, Class<? extends Event> type) {
        this.type = type;
        this.appMaker = appMaker;
    }

    /**
     * Name the stream.
     * 
     * @param name
     *            the stream name, default is an empty string.
     * @return the stream maker object
     */
    public StreamMaker withName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Define the key finder for this stream.
     * 
     * @param keyFinder
     *            a function to lookup the value of the key.
     * @return the stream maker object
     */
    public StreamMaker withKey(KeyFinder<?> keyFinder) {
        this.keyFinder = keyFinder;
        this.keyFinderString = null;
        return this;
    }

    /**
     * Define the key finder for this stream using a descriptor.
     * 
     * @param keyFinderString
     *            a descriptor to lookup the value of the key.
     * @return the stream maker object
     */
    public StreamMaker withKey(String keyFinderString) {
        this.keyFinder = null;
        this.keyFinderString = keyFinderString;
        return this;
    }

    /**
     * Define the key finder for this stream using a descriptor.
     * 
     * @param keyFinderString
     *            a descriptor to lookup the value of the key.
     * @return the stream maker object
     */
    public StreamMaker to(PEMaker pem) {
        appMaker.add(this, pem);
        this.pem = pem;
        return this;
    }

    private Stream<? extends Event> getStream() {

        // Stream stream = new Stream(appMaker, name, getKeyFinder(), getProcessingElements());
        return null;// stream;
    }

    private KeyFinder<?> getKeyFinder() {

        return null;
    }

    private ProcessingElement[] getProcessingElements() {
        return null;
    }
}
