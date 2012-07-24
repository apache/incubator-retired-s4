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

package org.apache.s4.deploy;

import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.tcp.TCPListener;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterFromZK;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

public class TestModule extends AbstractModule {

    private PropertiesConfiguration config;

    private void loadProperties(Binder binder) {

        try {
            InputStream is = this.getClass().getResourceAsStream("/org.apache.s4.deploy.s4.properties");
            config = new PropertiesConfiguration();
            config.load(is);
            System.out.println(ConfigurationUtils.toString(config));
            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));
        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        bind(Cluster.class);
        bind(Hasher.class).to(DefaultHasher.class);
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(Assignment.class).to(AssignmentFromZK.class);
        bind(Cluster.class).to(ClusterFromZK.class);
        bind(Emitter.class).to(TCPEmitter.class);
        bind(Listener.class).to(TCPListener.class);

        bind(Integer.class).annotatedWith(Names.named("comm.retries")).toInstance(10);
        bind(Integer.class).annotatedWith(Names.named("comm.retry_delay")).toInstance(10);
        bind(Integer.class).annotatedWith(Names.named("comm.timeout")).toInstance(1000);

        bind(Integer.class).annotatedWith(Names.named("tcp.partition.queue_size")).toInstance(256);
    }

}
