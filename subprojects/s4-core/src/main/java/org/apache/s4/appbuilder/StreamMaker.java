package org.apache.s4.appbuilder;

import org.apache.s4.base.Event;
import org.apache.s4.core.KeyFinder;

public class StreamMaker {

    private String name = "";
    private KeyFinder<?> keyFinder = null;
    private String keyFinderString;
    private PEMaker pem;
    private Class<? extends Event> type;

    /* Only package classes can instantiate this class. */
    StreamMaker(Class<? extends Event> type) {
        this.type = type;
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
    public StreamMaker withKeyFinder(KeyFinder<?> keyFinder) {
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
    public StreamMaker withKeyFinder(String keyFinderString) {
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
        this.pem = pem;
        return this;
    }

}
