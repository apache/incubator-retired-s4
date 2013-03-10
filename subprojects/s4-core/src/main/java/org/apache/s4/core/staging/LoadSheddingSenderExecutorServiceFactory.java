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
