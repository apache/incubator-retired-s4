package org.apache.s4.comm.staging;

import java.util.concurrent.Executor;

import org.apache.s4.comm.DeserializerExecutorFactory;
import org.slf4j.LoggerFactory;

/**
 * A factory for deserializer executors that returns a unique thread pool, shared among channels. This can be useful
 * when there are many inbound channels and that you need to limit the number of threads in the node.
 * 
 */
public class SharedThrottlingDeserializerExecutorFactory implements DeserializerExecutorFactory {

    private static final int QUEUE_CAPACITY = 100000;

    enum ThrottlingExecutorSingleton {
        INSTANCE;

        ThrottlingThreadPoolExecutorService executor;

        private ThrottlingExecutorSingleton() {
            this.executor = new ThrottlingThreadPoolExecutorService(Runtime.getRuntime().availableProcessors(), true,
                    "listener-deserializer-%d", QUEUE_CAPACITY, Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    public Executor create() {
        LoggerFactory
                .getLogger(getClass())
                .info("Creating a shared (i.e. singleton, shared across netty channel workers) throttling thread pool with queue capacity of {}",
                        QUEUE_CAPACITY);
        return ThrottlingExecutorSingleton.INSTANCE.executor;
    }
}
