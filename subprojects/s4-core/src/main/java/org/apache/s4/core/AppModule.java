package org.apache.s4.core;

import org.apache.s4.base.Listener;
import org.apache.s4.base.Receiver;
import org.apache.s4.base.Sender;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.TCPListener;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * This module is expected to be loaded with knowledge of the S4 appl classes. It can therefore bind dependencies which
 * also require knowledge of application classes (e.g. for deserialization).
 * 
 * This class is therefore loaded:
 * <ul>
 * <li>through the {@link S4RLoader} when the application is normally deployed / configured
 * <li>directly with the node classloader when the application classes are already in the classpath, for instance for
 * testing.
 * </ul>
 * 
 */
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
