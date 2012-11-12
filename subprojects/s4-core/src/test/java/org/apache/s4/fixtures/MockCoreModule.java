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

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.base.Sender;
import org.apache.s4.core.ReceiverImpl;
import org.apache.s4.core.staging.DefaultSenderExecutorServiceFactory;
import org.apache.s4.core.staging.DefaultStreamProcessingExecutorServiceFactory;
import org.apache.s4.core.staging.SenderExecutorServiceFactory;
import org.apache.s4.core.staging.StreamExecutorServiceFactory;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.NoOpDeploymentManager;
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
        bind(DeploymentManager.class).to(NoOpDeploymentManager.class);
        bind(Emitter.class).toInstance(Mockito.mock(Emitter.class));
        bind(Listener.class).toInstance(Mockito.mock(Listener.class));
        bind(ReceiverImpl.class).toInstance(Mockito.mock(ReceiverImpl.class));
        bind(Sender.class).toInstance(Mockito.mock(Sender.class));

        // Although we want to mock as much as possible, most tests still require the machinery for routing events
        // within a stream/node, therefore sender and stream executors are not mocked
        bind(StreamExecutorServiceFactory.class).to(DefaultStreamProcessingExecutorServiceFactory.class);

        bind(SenderExecutorServiceFactory.class).to(DefaultSenderExecutorServiceFactory.class);

        bind(Integer.class).annotatedWith(Names.named("s4.sender.parallelism")).toInstance(8);
        bind(Integer.class).annotatedWith(Names.named("s4.sender.workQueueSize")).toInstance(10000);

        bind(Integer.class).annotatedWith(Names.named("s4.stream.workQueueSize")).toInstance(10000);

    }
}
