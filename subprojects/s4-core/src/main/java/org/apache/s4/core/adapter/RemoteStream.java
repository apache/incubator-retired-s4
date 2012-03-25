package org.apache.s4.core.adapter;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.base.Event;
import org.apache.s4.core.RemoteSender;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream that dispatches events to a remote cluster
 * 
 */
public class RemoteStream implements Streamable<Event> {

    private Thread thread;
    String name;

    RemoteSender remoteSender;
    int id;
    private Adapter adapter;
    private static Logger logger = LoggerFactory.getLogger(RemoteStream.class);

    private static AtomicInteger remoteStreamCounter = new AtomicInteger();

    public RemoteStream(Adapter adapter, String name) {
        this.name = name;
        this.adapter = adapter;
        adapter.addStream(this);
        remoteSender = adapter.getRemoteSender();
        this.id = remoteStreamCounter.addAndGet(1);
    }

    @Override
    public void start() {

    }

    @Override
    public void put(Event event) {
        event.setStreamId(name);
        event.setAppId(adapter.getId());

        // TODO specify partitioning?
        remoteSender.sendToRemotePartitions(event);

    }

    @Override
    public void close() {
        thread.interrupt();
    }

    @Override
    public String getName() {
        return name;
    }

}
