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
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.PhysicalCluster;
import org.apache.s4.core.Receiver;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.NoOpDeploymentManager;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;

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
        bind(Receiver.class).toInstance(Mockito.mock(Receiver.class));
        Cluster clusterMock = Mockito.mock(Cluster.class);
        Mockito.when(clusterMock.getPhysicalCluster()).thenReturn(new PhysicalCluster(1));
        bind(Cluster.class).toInstance(clusterMock);
    }
}
