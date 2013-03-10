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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

public class ThrottlingThreadPoolExecutorService extends ForwardingListeningExecutorService {

    private static Logger logger = LoggerFactory.getLogger(ThrottlingThreadPoolExecutorService.class);

    int parallelism;
    String streamName;
    final ClassLoader classLoader;
    int workQueueSize;
    private BlockingQueue<Runnable> workQueue;
    private RateLimiter rateLimitedPermits;
    private ListeningExecutorService executorDelegatee;
    Meter droppingMeter;

    /**
     * 
     * @param parallelism
     *            Maximum number of threads in the pool
     * @param threadName
     *            Naming scheme
     * @param workQueueSize
     *            Queue capacity
     * @param classLoader
     *            ClassLoader used as contextClassLoader for spawned threads
     */
    public ThrottlingThreadPoolExecutorService(int parallelism, int rate, String threadName, int workQueueSize,
            final ClassLoader classLoader) {
        super();
        this.parallelism = parallelism;
        this.streamName = threadName;
        this.classLoader = classLoader;
        this.workQueueSize = workQueueSize;
        this.droppingMeter = Metrics.newMeter(getClass(), "throttling-dropping", "throttling-dropping",
                TimeUnit.SECONDS);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName)
                .setThreadFactory(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setContextClassLoader(classLoader);
                        return t;
                    }
                }).build();
        rateLimitedPermits = RateLimiter.create(rate);
        workQueue = new ArrayBlockingQueue<Runnable>(workQueueSize + parallelism);
        ThreadPoolExecutor eventProcessingExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60,
                TimeUnit.SECONDS, workQueue, threadFactory, new RejectedExecutionHandler() {

                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        droppingMeter.mark();
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
        rateLimitedPermits.acquire();
        ListenableFuture<T> future = super.submit(task);
        return future;
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
        rateLimitedPermits.acquire();
        ListenableFuture<T> future = super.submit(task, result);
        return future;
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        rateLimitedPermits.acquire();
        ListenableFuture<?> future = super.submit(task);
        return future;
    }

    @Override
    public void execute(Runnable command) {
        rateLimitedPermits.acquire();
        super.execute(command);
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
