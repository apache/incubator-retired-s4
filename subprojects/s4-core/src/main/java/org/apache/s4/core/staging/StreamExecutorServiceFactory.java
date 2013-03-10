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

import org.apache.s4.core.App;

/**
 * Factory for creating an executor service that will process events in PEs. This is typically done asynchronously with
 * a configurable thread pool.
 * <p>
 * Implementations may use dependency injection to set some default parameters.
 * <p>
 * Implementations may rely on different strategies for handling high loads: blocking, throttling, dropping and that may
 * also be provided on a per-stream basis (based on the name of the stream for instance).
 */
public interface StreamExecutorServiceFactory {

    /**
     * Creates the executor service for a given stream.
     * 
     * @param parallelism
     *            Number of concurrent threads
     * @param name
     *            Name of the stream (for naming threads)
     * @param classLoader
     *            Classloader used for specifying the context classloader in processing threads. This is usually the
     *            classloader that loaded the {@link App} class.
     * @return Executor service for processing events in PEs
     */
    ExecutorService create(int parallelism, String name, ClassLoader classLoader);
}
