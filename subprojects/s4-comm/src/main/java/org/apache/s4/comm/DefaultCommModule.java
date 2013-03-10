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

package org.apache.s4.comm;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.staging.BlockingDeserializerExecutorFactory;
import org.apache.s4.comm.tcp.DefaultRemoteEmitters;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterFromZK;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.ClustersFromZK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * Default configuration module for the communication layer. Parameterizable / overridable through custom modules and /
 * or configuration file (for string literals).
 * 
 */
public class DefaultCommModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(DefaultCommModule.class);
    InputStream commConfigInputStream;
    private PropertiesConfiguration config;

    /**
     * 
     * @param commConfigInputStream
     *            input stream from a configuration file
     */
    public DefaultCommModule(InputStream commConfigInputStream) {
        super();
        this.commConfigInputStream = commConfigInputStream;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        if (commConfigInputStream != null) {
            try {
                commConfigInputStream.close();
            } catch (IOException ignored) {
            }
        }

        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);
        /* Use Kryo to serialize events. */
        // we use a factory for generating the serdeser instance in order to use runtime parameters such as the
        // classloader
        install(new FactoryModuleBuilder().implement(SerializerDeserializer.class, KryoSerDeser.class).build(
                SerializerDeserializerFactory.class));

        bind(Cluster.class).to(ClusterFromZK.class);

        bind(Clusters.class).to(ClustersFromZK.class);

        bind(DeserializerExecutorFactory.class).to(BlockingDeserializerExecutorFactory.class);

        try {
            Class<? extends Emitter> emitterClass = (Class<? extends Emitter>) Class.forName(config
                    .getString("s4.comm.emitter.class"));

            bind(Emitter.class).to(emitterClass).in(Scopes.SINGLETON);

            // RemoteEmitter instances are created through a factory, depending on the topology. We inject the factory
            Class<? extends RemoteEmitter> remoteEmitterClass = (Class<? extends RemoteEmitter>) Class.forName(config
                    .getString("s4.comm.emitter.remote.class"));
            install(new FactoryModuleBuilder().implement(RemoteEmitter.class, remoteEmitterClass).build(
                    RemoteEmitterFactory.class));
            bind(RemoteEmitters.class).to(DefaultRemoteEmitters.class);

        } catch (ClassNotFoundException e) {
            logger.error("Cannot find class implementation ", e);
        }
    }

    private void loadProperties(Binder binder) {
        try {
            config = new PropertiesConfiguration();
            config.load(commConfigInputStream);

            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));

        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

}
