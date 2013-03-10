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

import java.util.concurrent.ExecutorService;

import org.apache.s4.comm.staging.BlockingThreadPoolExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Factory for stream executors that block when task queue is full.
 * 
 */
public class BlockingStreamExecutorServiceFactory implements StreamExecutorServiceFactory {

    private final int workQueueSize;

    @Inject
    public BlockingStreamExecutorServiceFactory(@Named("s4.stream.workQueueSize") int workQueueSize) {
        this.workQueueSize = workQueueSize;
    }

    @Override
    public ExecutorService create(int parallelism, String name, ClassLoader classLoader) {
        return new BlockingThreadPoolExecutorService(parallelism, false, name, workQueueSize, classLoader);
    }

}
