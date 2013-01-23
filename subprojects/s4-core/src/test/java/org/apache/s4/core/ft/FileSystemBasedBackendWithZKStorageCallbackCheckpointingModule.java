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

package org.apache.s4.core.ft;

import org.apache.s4.core.ft.CheckpointingFramework.StorageResultCode;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Creates the /checkpointed znode if a successful checkpointing callback is received. Does it only once.
 * 
 */
public class FileSystemBasedBackendWithZKStorageCallbackCheckpointingModule extends
        FileSystemBackendCheckpointingModule {

    @Override
    protected void configure() {
        super.configure();
        bind(StorageCallbackFactory.class).to(DummyZKStorageCallbackFactory.class);

    }

    public static class DummyZKStorageCallbackFactory implements StorageCallbackFactory {

        @Override
        public StorageCallback createStorageCallback() {
            return new StorageCallback() {

                @Override
                public void storageOperationResult(StorageResultCode resultCode, Object message) {
                    if (resultCode == StorageResultCode.SUCCESS) {
                        try {
                            ZooKeeper zkClient = CoreTestUtils.createZkClient();
                            zkClient.create("/checkpointed", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                };
            };

        }

    }
}
