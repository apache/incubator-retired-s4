package org.apache.s4.comm;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Default executor factory implementation for deserialization stage.
 * 
 * 
 * 
 */
public class DefaultDeserializerExecutorFactory implements DeserializerExecutorFactory {

    @Named("s4.listener.maxMemoryPerChannel")
    @Inject(optional = true)
    int maxMemoryPerChannel = 100000;

    @Named("s4.listener.maxMemoryPerExecutor")
    @Inject(optional = true)
    int maxMemoryPerExecutor = 100000;

    @Override
    public Executor create() {
        // NOTE: these are suggested defaults but they might require application-specific tuning
        return new OrderedMemoryAwareThreadPoolExecutor(1, maxMemoryPerChannel, maxMemoryPerExecutor, 60,
                TimeUnit.SECONDS, new ThreadFactoryBuilder().setNameFormat("listener-deserializer-%d").build());
    }
}
