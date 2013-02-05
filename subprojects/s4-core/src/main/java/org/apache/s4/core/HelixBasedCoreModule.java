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

import org.apache.s4.comm.topology.HelixRemoteStreams;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.HelixBasedDeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

/**
 * Temporary module allowing assignment from ZK, communication through Netty, and distributed deployment management,
 * until we have a better way to customize node configuration
 * 
 */
public class HelixBasedCoreModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(HelixBasedCoreModule.class);

    public HelixBasedCoreModule() {
    }

    @Override
    protected void configure() {

        bind(DeploymentManager.class).to(HelixBasedDeploymentManager.class).in(Scopes.SINGLETON);

        bind(RemoteStreams.class).to(HelixRemoteStreams.class).in(Scopes.SINGLETON);
        bind(RemoteSenders.class).to(DefaultRemoteSenders.class).in(Scopes.SINGLETON);

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

}
