package org.apache.s4.core.adapter;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.RemoteSender;

import com.google.inject.Inject;

public abstract class Adapter extends App {

    @Inject
    RemoteSender remoteSender;

    public RemoteSender getRemoteSender() {
        return remoteSender;
    }

    public void setRemoteSender(RemoteSender remoteSender) {
        this.remoteSender = remoteSender;
    }

    protected <T extends Event> RemoteStream createRemoteStream(String name) {

        return new RemoteStream(this, name);
    }
}
