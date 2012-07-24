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

import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.Clusters;

import com.google.inject.AbstractModule;

/**
 * 
 * Used for injecting non-fail-fast zookeeper client classes.
 * 
 * Here is why:
 * 
 * <ul>
 * <li>tests contained in a single junit class are not forked: forking is on a class basis</li>
 * <li>zookeeper client classes are injected during the tests</li>
 * <li>zookeeper server is restarted between test methods.</li>
 * <li>zookeeper client classes from previous tests methods get a "expired" exception upon reconnection to the new
 * zookeeper instance. With a fail-fast implementation, this would kill the current test.</li>
 * </ul>
 * 
 * 
 */
public class NonFailFastZookeeperClientsModule extends AbstractModule {

    public NonFailFastZookeeperClientsModule() {
    }

    @Override
    protected void configure() {
        bind(Assignment.class).to(AssignmentFromZKNoFailFast.class);
        bind(Cluster.class).to(ClusterFromZKNoFailFast.class);

        bind(Clusters.class).to(ClustersFromZKNoFailFast.class);

    }

}
