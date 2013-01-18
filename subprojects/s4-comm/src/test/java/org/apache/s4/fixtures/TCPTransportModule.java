package org.apache.s4.fixtures;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.tcp.TCPListener;

import com.google.inject.AbstractModule;

public class TCPTransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Listener.class).to(TCPListener.class);
        bind(Emitter.class).to(TCPEmitter.class);
    }

}
