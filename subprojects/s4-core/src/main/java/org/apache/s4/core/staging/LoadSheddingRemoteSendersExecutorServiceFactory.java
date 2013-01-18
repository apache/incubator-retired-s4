package org.apache.s4.core.staging;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Load shedding implementation of the remote senders executor factory. It clones the implementation of the
 * {@link LoadSheddingSenderExecutorServiceFactory} class.
 * 
 */
public class LoadSheddingRemoteSendersExecutorServiceFactory extends LoadSheddingSenderExecutorServiceFactory implements
        RemoteSendersExecutorServiceFactory {

    @Inject
    public LoadSheddingRemoteSendersExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        super(threadPoolSize, workQueueSize);
    }

}
