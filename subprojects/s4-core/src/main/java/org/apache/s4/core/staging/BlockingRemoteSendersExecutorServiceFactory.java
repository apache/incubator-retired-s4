package org.apache.s4.core.staging;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Blocking implementation of the remote senders executor factory. It clones the implementation of the
 * {@link BlockingSenderExecutorServiceFactory} class.
 * 
 */
public class BlockingRemoteSendersExecutorServiceFactory extends BlockingSenderExecutorServiceFactory implements
        RemoteSendersExecutorServiceFactory {

    @Inject
    public BlockingRemoteSendersExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        super(threadPoolSize, workQueueSize);
    }

}
