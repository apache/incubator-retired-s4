package org.apache.s4.core;

import org.apache.s4.base.Listener;
import org.apache.s4.base.Receiver;
import org.apache.s4.base.Sender;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPListener;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class AppModule extends AbstractModule {

    ClassLoader appClassLoader;

    public AppModule(ClassLoader appClassLoader) {
        this.appClassLoader = appClassLoader;
    }

    @Provides
    public SerializerDeserializer provideSerializerDeserializer(SerializerDeserializerFactory serDeserFactory) {
        return serDeserFactory.createSerializerDeserializer(appClassLoader);
    }

    @Override
    protected void configure() {
        // bind(S4Metrics.class);

        bind(Receiver.class).to(ReceiverImpl.class);
        bind(Sender.class).to(SenderImpl.class);

        // TODO allow pluggable transport implementation
        bind(Listener.class).to(TCPListener.class);

    }

}
