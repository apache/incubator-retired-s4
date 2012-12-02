package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.staging.ThrottlingThreadPoolExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Default factory implementation for the sender executor service. It uses a mechanism for throttling the submission of
 * events and maintaining partial order.
 * 
 */
public class DefaultSenderExecutorServiceFactory implements SenderExecutorServiceFactory {

    private final int threadPoolSize;
    private final int workQueueSize;

    @Inject
    public DefaultSenderExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        this.threadPoolSize = threadPoolSize;
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create() {
        return new ThrottlingThreadPoolExecutorService(threadPoolSize, true,
                (this instanceof DefaultRemoteSendersExecutorServiceFactory) ? "remote-sender-%d" : "sender-%d",
                workQueueSize, getClass().getClassLoader());

    }
}
