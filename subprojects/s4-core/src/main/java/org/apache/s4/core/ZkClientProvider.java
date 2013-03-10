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

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * 
 * Provides a connection to ZooKeeper through the {@link ZkClient} class.
 * 
 * As an application singleton, it provides a single shared connection for an S4 node.
 * 
 */
@Singleton
// injected only once per node
public class ZkClientProvider implements Provider<ZkClient> {

    private final ZkClient zkClient;

    @Inject
    public ZkClientProvider(@Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout) {
        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
    }

    @Override
    public ZkClient get() {
        // reuses initialized instance
        return zkClient;

    }
}
