package org.apache.s4.core.staging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.s4.core.util.S4Metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * <p>
 * Load shedding factory for the event processing stage executors.
 * <p>
 * It provides optional parallelism, when the processing activity requires blocking I/O operations, or is CPU-bound.
 * <p>
 * It drops events when work queue is full.
 * <p>
 * More customized load shedding strategies can be defined based on this simple implementation.
 * 
 */
public class LoadSheddingStreamExecutorServiceFactory implements StreamExecutorServiceFactory {

    private final int workQueueSize;

    @Inject
    private S4Metrics metrics;

    @Inject
    public LoadSheddingStreamExecutorServiceFactory(@Named("s4.stream.workQueueSize") int workQueueSize) {
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create(int parallelism, final String name, final ClassLoader classLoader) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("stream-" + name + "-%d").setThreadFactory(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setContextClassLoader(classLoader);
                        return t;
                    }
                }).build();
        RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                metrics.droppedEvent(name);
            }
        };
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(workQueueSize), threadFactory, rejectedExecutionHandler);
        tpe.allowCoreThreadTimeOut(true);
        return tpe;
    }
}
