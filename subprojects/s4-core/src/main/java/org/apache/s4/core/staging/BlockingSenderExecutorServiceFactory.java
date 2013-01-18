package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.staging.BlockingThreadPoolExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Blocking factory implementation for the sender executor service. It uses a mechanism that blocks the submission of
 * events when the work queue is full.
 * <p>
 * Beware that this can lead to deadlocks if processing queues are full on all nodes.
 * 
 */
public class BlockingSenderExecutorServiceFactory implements SenderExecutorServiceFactory {

    private final int threadPoolSize;
    private final int workQueueSize;

    @Inject
    public BlockingSenderExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        this.threadPoolSize = threadPoolSize;
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create() {
        return new BlockingThreadPoolExecutorService(threadPoolSize, false,
                (this instanceof RemoteSendersExecutorServiceFactory) ? "remote-sender-%d" : "sender-%d",
                workQueueSize, getClass().getClassLoader());
    }
}
