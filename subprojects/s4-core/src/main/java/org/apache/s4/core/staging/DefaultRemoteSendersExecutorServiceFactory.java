package org.apache.s4.core.staging;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Default implementation of the remote senders executor factory. It clones the implementation of the
 * {@link DefaultSenderExecutorServiceFactory} class.
 * 
 */
public class DefaultRemoteSendersExecutorServiceFactory extends DefaultSenderExecutorServiceFactory implements
        RemoteSendersExecutorServiceFactory {

    @Inject
    public DefaultRemoteSendersExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        super(threadPoolSize, workQueueSize);
    }

}
