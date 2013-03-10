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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.s4.comm.DeserializerExecutorFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Executors factory for the deserialization stage that blocks incoming tasks when memory consumed by events reaches a
 * given threshold.
 * <p>
 * It uses a memory-aware threadpool single threaded executor. There is 1 threadpool for each channel.
 * <p>
 * The memory limit for the threadpool can be configured per channel and per executor (with a single threaded executor,
 * the lowest of those value is the one that mandates)
 * 
 * 
 * 
 */
public class MemoryAwareDeserializerExecutorFactory implements DeserializerExecutorFactory {

    @Named("s4.listener.maxMemoryPerChannel")
    @Inject(optional = true)
    protected int maxMemoryPerChannel = 1000000;

    @Named("s4.listener.maxMemoryPerExecutor")
    @Inject(optional = true)
    protected int maxMemoryPerExecutor = 1000000;

    @Override
    public Executor create() {
        LoggerFactory.getLogger(getClass()).info(
                "Creating an OMATPE with maxmemperchannel= {} and maxmemperexecutor= {}", maxMemoryPerChannel,
                maxMemoryPerExecutor);
        return new OrderedMemoryAwareThreadPoolExecutor(1, maxMemoryPerChannel, maxMemoryPerExecutor, 60,
                TimeUnit.SECONDS, new ThreadFactoryBuilder().setNameFormat("listener-deserializer-%d").build());
    }
}
