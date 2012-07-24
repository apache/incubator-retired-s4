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

import org.apache.s4.core.ft.FileSystemBasedBackendWithZKStorageCallbackCheckpointingModule.DummyZKStorageCallbackFactory;
import org.apache.s4.fixtures.CommTestUtils;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class CheckpointingModuleWithUnrespondingFetchingStorageBackend extends AbstractModule {

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named("s4.checkpointing.filesystem.storageRootPath")).toInstance(
                CommTestUtils.DEFAULT_STORAGE_DIR.getAbsolutePath());
        bind(StateStorage.class).to(StorageWithUnrespondingFetching.class);
        bind(CheckpointingFramework.class).to(SafeKeeper.class);
        bind(StorageCallbackFactory.class).to(DummyZKStorageCallbackFactory.class);
    }

}
