package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.core.App;

/**
 * Factory for creating an executor service that will process events in PEs. This is typically done asynchronously with
 * a configurable thread pool.
 * <p>
 * Implementations may use dependency injection to set some default parameters.
 * <p>
 * Implementations may rely on different strategies for handling high loads: blocking, throttling, dropping and that may
 * also be provided on a per-stream basis (based on the name of the stream for instance).
 */
public interface StreamExecutorServiceFactory {

    /**
     * Creates the executor service for a given stream.
     * 
     * @param parallelism
     *            Number of concurrent threads
     * @param name
     *            Name of the stream (for naming threads)
     * @param classLoader
     *            Classloader used for specifying the context classloader in processing threads. This is usually the
     *            classloader that loaded the {@link App} class.
     * @return Executor service for processing events in PEs
     */
    ExecutorService create(int parallelism, String name, ClassLoader classLoader);
}
