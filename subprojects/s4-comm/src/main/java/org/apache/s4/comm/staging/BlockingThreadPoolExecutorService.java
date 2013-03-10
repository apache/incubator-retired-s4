/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.comm.staging;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This thread pool executor throttles the submission of new tasks by using a semaphore. Task submissions require
 * permits, task completions release permits.
 * <p>
 * NOTE: you should either use the {@link BlockingThreadPoolExecutorService#submit(java.util.concurrent.Callable)}
 * methods or the {@link BlockingThreadPoolExecutorService#execute(Runnable)} method.
 * 
 */
public class BlockingThreadPoolExecutorService extends ForwardingListeningExecutorService {

    private static Logger logger = LoggerFactory.getLogger(BlockingThreadPoolExecutorService.class);

    int parallelism;
    String streamName;
    final ClassLoader classLoader;
    int workQueueSize;
    private BlockingQueue<Runnable> workQueue;
    private Semaphore queueingPermits;
    private ListeningExecutorService executorDelegatee;

    /**
     * 
     * @param parallelism
     *            Maximum number of threads in the pool
     * @param fairParallelism
     *            If true, in case of contention, waiting threads will be scheduled in a first-in first-out manner. This
     *            can be help ensure ordering, though there is an associated performance cost (typically small).
     * @param threadName
     *            Naming scheme
     * @param workQueueSize
     *            Queue capacity
     * @param classLoader
     *            ClassLoader used as contextClassLoader for spawned threads
     */
    public BlockingThreadPoolExecutorService(int parallelism, boolean fairParallelism, String threadName,
            int workQueueSize, final ClassLoader classLoader) {
        super();
        this.parallelism = parallelism;
        this.streamName = threadName;
        this.classLoader = classLoader;
        this.workQueueSize = workQueueSize;
        queueingPermits = new Semaphore(workQueueSize + parallelism, false);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName)
                .setThreadFactory(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setContextClassLoader(classLoader);
                        return t;
                    }
                }).build();
        // queueingPermits semaphore controls the size of the queue, thus no need to use a bounded queue
        workQueue = new LinkedBlockingQueue<Runnable>(workQueueSize + parallelism);
        ThreadPoolExecutor eventProcessingExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60,
                TimeUnit.SECONDS, workQueue, threadFactory, new RejectedExecutionHandler() {

                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        // This is not expected to happen.
                        logger.error("Could not submit task to executor {}", executor.toString());
                    }
                });
        eventProcessingExecutor.allowCoreThreadTimeOut(true);
        executorDelegatee = MoreExecutors.listeningDecorator(eventProcessingExecutor);

    }

    @Override
    protected ListeningExecutorService delegate() {
        return executorDelegatee;
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        ListenableFuture<T> future = super.submit(new CallableWithPermitRelease<T>(task));
        return future;
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        ListenableFuture<T> future = super.submit(new RunnableWithPermitRelease(task), result);
        return future;
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        ListenableFuture<?> future = super.submit(new RunnableWithPermitRelease(task));
        return future;
    }

    @Override
    public void execute(Runnable command) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.execute(new RunnableWithPermitRelease(command));
    }

    /**
     * Releases a permit after the task is executed
     * 
     */
    class RunnableWithPermitRelease implements Runnable {

        Runnable delegatee;

        public RunnableWithPermitRelease(Runnable delegatee) {
            this.delegatee = delegatee;
        }

        @Override
        public void run() {
            try {
                delegatee.run();
            } finally {
                queueingPermits.release();
            }

        }
    }

    /**
     * Releases a permit after the task is completed
     * 
     */
    class CallableWithPermitRelease<T> implements Callable<T> {

        Callable<T> delegatee;

        public CallableWithPermitRelease(Callable<T> delegatee) {
            this.delegatee = delegatee;
        }

        @Override
        public T call() throws Exception {
            try {
                return delegatee.call();
            } finally {
                queueingPermits.release();
            }
        }

    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new RuntimeException("Not implemented");
    }

}
