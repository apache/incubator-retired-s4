package org.apache.s4;

import org.apache.s4.base.Listener;

import com.google.inject.AbstractModule;

public class TestListenerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Listener.class).to(TestListener.class);
    }

}
