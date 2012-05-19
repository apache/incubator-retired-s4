package org.apache.s4.core;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.comm.topology.RemoteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream that dispatches events to a remote cluster
 * 
 */
public class RemoteStream implements Streamable<Event> {

    final private String name;
    final protected Key<Event> key;
    final static private String DEFAULT_SEPARATOR = "^";
    // final private int id;

    RemoteSenders remoteSenders;

    Hasher hasher;

    int id;
    private App app;
    private static Logger logger = LoggerFactory.getLogger(RemoteStream.class);

    private static AtomicInteger remoteStreamCounter = new AtomicInteger();

    public RemoteStream(App app, String name, KeyFinder<Event> finder, RemoteSenders remoteSenders, Hasher hasher,
            RemoteStreams remoteStreams, String clusterName) {
        this.app = app;
        this.name = name;
        this.remoteSenders = remoteSenders;
        this.hasher = hasher;
        if (finder == null) {
            this.key = null;
        } else {
            this.key = new Key<Event>(finder, DEFAULT_SEPARATOR);
        }
        remoteStreams.addOutputStream(String.valueOf(app.getId()), clusterName, name);

    }

    @Override
    public void put(Event event) {
        event.setStreamId(getName());
        event.setAppId(app.getId());

        if (key != null) {
            remoteSenders.send(key.get(event), event);
        } else {
            remoteSenders.send(null, event);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
