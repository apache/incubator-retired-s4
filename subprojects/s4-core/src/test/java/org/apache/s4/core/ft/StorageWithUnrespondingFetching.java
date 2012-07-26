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

import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class StorageWithUnrespondingFetching implements StateStorage {

    @Inject
    DefaultFileSystemStateStorage storage = new DefaultFileSystemStateStorage();

    @Inject
    @Named("s4.checkpointing.filesystem.storageRootPath")
    String storageRootPath;

    @Inject
    private void initStorageRootPath() {
        // manual init because we are not directly injecting the default file system storage, but creating a new
        // instance in this class
        storage.storageRootPath = this.storageRootPath;
    }

    @Override
    public void saveState(CheckpointId key, byte[] state, StorageCallback callback) {
        storage.saveState(key, state, callback);
    }

    @Override
    public byte[] fetchState(CheckpointId key) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return storage.fetchState(key);
    }

    @Override
    public Set<CheckpointId> fetchStoredKeys() {
        return storage.fetchStoredKeys();
    }

}
