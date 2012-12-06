package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.staging.ThrottlingThreadPoolExecutorService;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ThrottlingSenderExecutorServiceFactory implements SenderExecutorServiceFactory {

    private final int maxRate;
    private final int threadPoolSize;
    private final int workQueueSize;

    @Inject
    public ThrottlingSenderExecutorServiceFactory(@Named("s4.sender.maxRate") int maxRate,
            @Named("s4.sender.parallelism") int threadPoolSize, @Named("s4.sender.workQueueSize") int workQueueSize) {
        this.maxRate = maxRate;
        this.threadPoolSize = threadPoolSize;
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create() {
        LoggerFactory.getLogger(getClass()).info(
                "Creating a throttling executor with a pool size of {} and max rate of {} events / s",
                new String[] { String.valueOf(threadPoolSize), String.valueOf(maxRate), });
        return new ThrottlingThreadPoolExecutorService(threadPoolSize, maxRate,
                (this instanceof RemoteSendersExecutorServiceFactory) ? "remote-sender-%d" : "sender-%d",
                workQueueSize, getClass().getClassLoader());
    }
}
