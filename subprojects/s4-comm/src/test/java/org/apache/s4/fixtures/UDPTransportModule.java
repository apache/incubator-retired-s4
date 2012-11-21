package org.apache.s4.fixtures;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.comm.udp.UDPEmitter;
import org.apache.s4.comm.udp.UDPListener;

import com.google.inject.AbstractModule;

public class UDPTransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Listener.class).to(UDPListener.class);
        bind(Emitter.class).to(UDPEmitter.class);
    }

}
