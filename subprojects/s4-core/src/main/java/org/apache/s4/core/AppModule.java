package org.apache.s4.core;

import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;

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

    }

}
