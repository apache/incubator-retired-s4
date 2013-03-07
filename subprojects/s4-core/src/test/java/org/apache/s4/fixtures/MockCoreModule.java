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

package org.apache.s4.fixtures;

import org.apache.s4.comm.DeserializerExecutorFactory;
import org.apache.s4.comm.staging.MemoryAwareDeserializerExecutorFactory;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.core.RemoteSenders;
import org.apache.s4.core.staging.BlockingSenderExecutorServiceFactory;
import org.apache.s4.core.staging.BlockingStreamExecutorServiceFactory;
import org.apache.s4.core.staging.SenderExecutorServiceFactory;
import org.apache.s4.core.staging.StreamExecutorServiceFactory;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Core module mocking basic platform functionalities.
 * 
 */
public class MockCoreModule extends AbstractModule {

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(MockCoreModule.class);

    public MockCoreModule() {
    }

    @Override
    protected void configure() {
        // Although we want to mock as much as possible, most tests still require the machinery for routing events
        // within a stream/node, therefore sender and stream executors are not mocked

        // NOTE: we use a blocking executor so that events don't get dropped in simple tests
        bind(StreamExecutorServiceFactory.class).to(BlockingStreamExecutorServiceFactory.class);

        bind(SenderExecutorServiceFactory.class).to(BlockingSenderExecutorServiceFactory.class);
        bind(DeserializerExecutorFactory.class).to(MemoryAwareDeserializerExecutorFactory.class);

        bind(RemoteStreams.class).toInstance(Mockito.mock(RemoteStreams.class));
        bind(RemoteSenders.class).toInstance(Mockito.mock(RemoteSenders.class));

        bind(Integer.class).annotatedWith(Names.named("s4.sender.parallelism")).toInstance(8);
        bind(Integer.class).annotatedWith(Names.named("s4.sender.workQueueSize")).toInstance(10000);

        bind(Integer.class).annotatedWith(Names.named("s4.stream.workQueueSize")).toInstance(10000);
    }
}
