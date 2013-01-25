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
import org.apache.s4.base.Hasher;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.RemoteEmitterFactory;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.PhysicalCluster;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * Mock module for the comm layer. Mocks comm layer basic functionalities, and uses some default when required.
 * 
 */
public class MockCommModule extends AbstractModule {

    public MockCommModule() {
        super();
    }

    @Override
    protected void configure() {
        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);
        /* Use Kryo to serialize events. */
        install(new FactoryModuleBuilder().implement(SerializerDeserializer.class, KryoSerDeser.class).build(
                SerializerDeserializerFactory.class));
        bind(RemoteEmitters.class).toInstance(Mockito.mock(RemoteEmitters.class));
        bind(Clusters.class).toInstance(Mockito.mock(Clusters.class));
        Cluster mockedCluster = Mockito.mock(Cluster.class);
        Mockito.when(mockedCluster.getPhysicalCluster()).thenReturn(new PhysicalCluster(1));
        bind(Cluster.class).toInstance(mockedCluster);
        Assignment mockedAssignment = Mockito.mock(Assignment.class);
        Mockito.when(mockedAssignment.assignClusterNode()).thenReturn(new ClusterNode(0, 0, "machine", "Task-0"));
        bind(Assignment.class).toInstance(mockedAssignment);
        Names.bindProperties(binder(), ImmutableMap.of("s4.cluster.name", "testCluster", "s4.comm.timeout", "10000"));
        bind(Emitter.class).toInstance(Mockito.mock(Emitter.class));
        install(new FactoryModuleBuilder().implement(RemoteEmitter.class, Mockito.mock(RemoteEmitter.class).getClass())
                .build(RemoteEmitterFactory.class));

    }
}
