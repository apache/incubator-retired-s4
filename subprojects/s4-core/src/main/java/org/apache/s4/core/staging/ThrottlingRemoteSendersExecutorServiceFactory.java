package org.apache.s4.core.staging;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * 
 * Throttling implementation of the remote senders executor factory. It clones the implementation of the
 * {@link ThrottlingSenderExecutorServiceFactory} class.
 * 
 */
public class ThrottlingRemoteSendersExecutorServiceFactory extends ThrottlingSenderExecutorServiceFactory implements
        RemoteSendersExecutorServiceFactory {

    @Inject
    public ThrottlingRemoteSendersExecutorServiceFactory(@Named("s4.remoteSender.maxRate") int maxRate,
            @Named("s4.remoteSender.parallelism") int threadPoolSize,
            @Named("s4.remoteSender.workQueueSize") int workQueueSize) {
        super(maxRate, threadPoolSize, workQueueSize);
    }

}
