package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.staging.BlockingThreadPoolExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class BlockingStreamExecutorServiceFactory implements StreamExecutorServiceFactory {

    private final int workQueueSize;

    @Inject
    public BlockingStreamExecutorServiceFactory(@Named("s4.stream.workQueueSize") int workQueueSize) {
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create(int parallelism, String name, ClassLoader classLoader) {
        return new BlockingThreadPoolExecutorService(1, false, name, workQueueSize, classLoader);
    }

}
