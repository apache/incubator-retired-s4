package org.apache.s4.core.staging;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ThrottlingRemoteSendersExecutorServiceFactory extends ThrottlingSenderExecutorServiceFactory implements
        RemoteSendersExecutorServiceFactory {

    @Inject
    public ThrottlingRemoteSendersExecutorServiceFactory(@Named("s4.sender.maxRate") int maxRate,
            @Named("s4.sender.parallelism") int threadPoolSize, @Named("s4.sender.workQueueSize") int workQueueSize) {
        super(maxRate, threadPoolSize, workQueueSize);
    }

}
