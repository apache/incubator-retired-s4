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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.util.S4RLoaderFactory;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.comm.topology.ZkRemoteStreams;
import org.apache.s4.core.ft.CheckpointingFramework;
import org.apache.s4.core.ft.NoOpCheckpointingFramework;
import org.apache.s4.core.staging.BlockingRemoteSendersExecutorServiceFactory;
import org.apache.s4.core.staging.BlockingStreamExecutorServiceFactory;
import org.apache.s4.core.staging.RemoteSendersExecutorServiceFactory;
import org.apache.s4.core.staging.SenderExecutorServiceFactory;
import org.apache.s4.core.staging.StreamExecutorServiceFactory;
import org.apache.s4.core.staging.ThrottlingSenderExecutorServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/**
 * This module binds the different services required by an app, except for the connectivity to the cluster manager and
 * the communication layer.
 * 
 */
public class DefaultCoreModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(DefaultCoreModule.class);

    InputStream coreConfigFileInputStream;
    private PropertiesConfiguration config;

    String clusterName = null;

    public DefaultCoreModule(InputStream coreConfigFileInputStream) {
        this.coreConfigFileInputStream = coreConfigFileInputStream;
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        if (coreConfigFileInputStream != null) {
            try {
                coreConfigFileInputStream.close();
            } catch (IOException ignored) {
            }
        }

        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);

        bind(S4RLoaderFactory.class);

        // For enabling checkpointing, one needs to use a custom module, such as
        // org.apache.s4.core.ft.FileSytemBasedCheckpointingModule
        bind(CheckpointingFramework.class).to(NoOpCheckpointingFramework.class);

        // shed load in local sender only by default
        bind(SenderExecutorServiceFactory.class).to(ThrottlingSenderExecutorServiceFactory.class);
        bind(RemoteSendersExecutorServiceFactory.class).to(BlockingRemoteSendersExecutorServiceFactory.class);

        bind(StreamExecutorServiceFactory.class).to(BlockingStreamExecutorServiceFactory.class);

        bind(RemoteStreams.class).to(ZkRemoteStreams.class);
        bind(RemoteSenders.class).to(DefaultRemoteSenders.class);

    }

    @Provides
    @Named("s4.tmp.dir")
    public File provideTmpDir() {
        File tmpS4Dir = Files.createTempDir();
        tmpS4Dir.deleteOnExit();
        logger.warn(
                "s4.tmp.dir not specified, using temporary directory [{}] for unpacking S4R. You may want to specify a parent non-temporary directory.",
                tmpS4Dir.getAbsolutePath());
        return tmpS4Dir;
    }

    private void loadProperties(Binder binder) {
        try {
            config = new PropertiesConfiguration();
            config.load(coreConfigFileInputStream);

            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));

        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

}
