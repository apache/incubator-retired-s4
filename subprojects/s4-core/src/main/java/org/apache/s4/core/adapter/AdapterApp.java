package org.apache.s4.core.adapter;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.RemoteStream;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Base class for adapters. For now, it provides facilities for automatically creating an output stream.
 * 
 */
public abstract class AdapterApp extends App {

    @Inject
    @Named(value = "adapter.output.stream")
    String outputStreamName;

    private RemoteStream remoteStream;

    protected KeyFinder<Event> remoteStreamKeyFinder;

    public RemoteStream getRemoteStream() {
        return remoteStream;
    }

    @Override
    protected void onStart() {
    }

    @Override
    protected void onInit() {
        remoteStream = createOutputStream(outputStreamName, remoteStreamKeyFinder);
    }

    /**
     * This method allows to specify a keyfinder in order to partition the output stream
     * 
     * @param keyFinder
     *            used for identifying keys from the events
     */
    protected void setKeyFinder(KeyFinder<Event> keyFinder) {
        this.remoteStreamKeyFinder = keyFinder;
    }

    @Override
    protected void onClose() {
    }

}
