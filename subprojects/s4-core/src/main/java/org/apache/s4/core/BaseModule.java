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
package org.apache.s4.core;

import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.ArchiveFetcher;
import org.apache.s4.core.util.RemoteFileFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

/**
 * This module binds the minimum set of classes required for a node to "bootstrap", i.e. connect to the cluster manager
 * and be able to read and fetch configuration data.
 */
public class BaseModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(BaseModule.class);

    private PropertiesConfiguration config;
    InputStream baseConfigInputStream;
    String clusterName;

    public BaseModule(InputStream baseConfigInputStream, String clusterName) {
        super();
        this.baseConfigInputStream = baseConfigInputStream;
        this.clusterName = clusterName;
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        // a node holds a single partition assignment
        // ==> Assignment is a singleton so it shared between base, comm and app layers.
        // it is eager so that the node is able to join a cluster immediately
        bind(Assignment.class).to(AssignmentFromZK.class).asEagerSingleton();
        // bind(Cluster.class).to(ClusterFromZK.class);

        bind(ArchiveFetcher.class).to(RemoteFileFetcher.class);
        bind(S4Bootstrap.class);

        // ZkClientProvider singleton shares the Zookeeper connection
        bind(ZkClient.class).toProvider(ZkClientProvider.class);

    }

    @SuppressWarnings("serial")
    private void loadProperties(Binder binder) {
        try {
            config = new PropertiesConfiguration();
            config.load(baseConfigInputStream);

            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));

            if (clusterName != null) {
                if (config.containsKey("s4.cluster.name")) {
                    logger.warn(
                            "cluster [{}] passed as a parameter will not be used because an existing cluster.name parameter of value [{}] was found in the configuration file and will be used",
                            clusterName, config.getProperty("s4.cluster.name"));
                } else {
                    Names.bindProperties(binder, new HashMap<String, String>() {
                        {
                            put("s4.cluster.name", clusterName);
                        }
                    });
                }
            }

        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }
}
