package org.apache.s4.core.staging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.s4.core.util.S4Metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Factory for sender executors that drops events if the communication channel is full. (Typically because
 * 
 */
public class LoadSheddingSenderExecutorServiceFactory implements SenderExecutorServiceFactory {

    private final int workQueueSize;

    @Inject
    private S4Metrics metrics;

    private final int parallelism;

    @Inject
    public LoadSheddingSenderExecutorServiceFactory(@Named("s4.sender.parallelism") int threadPoolSize,
            @Named("s4.sender.workQueueSize") int workQueueSize) {
        this.workQueueSize = workQueueSize;
        this.parallelism = threadPoolSize;
    }

    @Override
    public ExecutorService create() {
        boolean remote = (this instanceof RemoteSendersExecutorServiceFactory);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat(remote ? "remote-sender-%d" : "sender-%d").build();

        RejectedExecutionHandler rejectedExecutionHandler = (remote ? new RejectedExecutionHandler() {

            // from a remote sender
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                metrics.droppedEventInRemoteSender();
            }
        } : new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                metrics.droppedEventInSender();
            }
        });
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(workQueueSize), threadFactory, rejectedExecutionHandler);
        tpe.allowCoreThreadTimeOut(true);
        return tpe;
    }

}
