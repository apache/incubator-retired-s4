package org.apache.s4.comm.staging;

import java.util.concurrent.Executor;

import org.apache.s4.comm.DeserializerExecutorFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Executors factory for the deserialization stage that blocks incoming tasks when the work queue is full.
 * 
 */
public class BlockingDeserializerExecutorFactory implements DeserializerExecutorFactory {

    @Named("s4.listener.maxEventsPerDeserializer")
    @Inject(optional = true)
    protected int maxEventsPerDeserializer = 100000;

    @Override
    public Executor create() {
        return new BlockingThreadPoolExecutorService(1, false, "deserializer-%d", maxEventsPerDeserializer, getClass()
                .getClassLoader());
    }

}
