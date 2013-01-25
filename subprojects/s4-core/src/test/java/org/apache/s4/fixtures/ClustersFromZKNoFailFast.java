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

import org.apache.s4.comm.topology.ClustersFromZK;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ClustersFromZKNoFailFast extends ClustersFromZK {

    @Inject
    public ClustersFromZKNoFailFast(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, ZkClient zkClient) throws Exception {
        super(clusterName, connectionTimeout, zkClient);
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        // no fail fast
    }

}
