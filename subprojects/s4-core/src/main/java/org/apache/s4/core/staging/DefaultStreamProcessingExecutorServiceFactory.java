package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.ThrottlingThreadPoolExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * <p>
 * Default factory for the event processing stage executors.
 * </p>
 * <p>
 * It provides optional parallelism, when the processing activity requires blocking I/O operations, or is CPU-bound.
 * </p>
 * <p>
 * It throttles the submission of events while preserving partial ordering.
 * </p>
 * 
 */
public class DefaultStreamProcessingExecutorServiceFactory implements StreamExecutorServiceFactory {

    private int workQueueSize;

    @Inject
    public DefaultStreamProcessingExecutorServiceFactory(@Named("s4.stream.workQueueSize") int workQueueSize) {
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create(int parallelism, String name, final ClassLoader classLoader) {
        return new ThrottlingThreadPoolExecutorService(parallelism, true, "stream-" + name + "-%d", workQueueSize,
                classLoader);

    }
}
