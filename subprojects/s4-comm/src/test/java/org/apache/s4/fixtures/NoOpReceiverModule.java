package org.apache.s4.fixtures;

import org.apache.s4.base.Receiver;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Avoids delegating message processing to the application layer.
 * 
 */
public class NoOpReceiverModule extends AbstractModule {

    @Provides
    public SerializerDeserializer provideSerializerDeserializer(SerializerDeserializerFactory serDeserFactory) {
        // we use the current classloader here, no app class to serialize
        return serDeserFactory.createSerializerDeserializer(getClass().getClassLoader());
    }

    @Override
    protected void configure() {
        bind(Receiver.class).to(NoOpReceiver.class);
    }

}
