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

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;

/**
 * Checkpointing module that uses the {@link DefaultFileSystemStateStorage} as a checkpointing backend.
 * 
 */
public class FileSystemBackendCheckpointingModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(FileSystemBackendCheckpointingModule.class);

    @Override
    protected void configure() {
        bind(StateStorage.class).to(DefaultFileSystemStateStorage.class);
        bind(CheckpointingFramework.class).to(SafeKeeper.class);

    }

    @Provides
    @Named("s4.checkpointing.filesystem.storageRootPath")
    public String provideStorageRootPath() {
        File defaultStorageDir = new File(System.getProperty("user.dir") + File.separator + "tmp" + File.separator
                + "storage");
        String storageRootPath = defaultStorageDir.getAbsolutePath();
        logger.warn("Unspecified storage dir; using default dir: {}", defaultStorageDir.getAbsolutePath());
        if (!defaultStorageDir.exists()) {
            if (!(defaultStorageDir.mkdirs())) {
                logger.error("Storage directory not specified, and cannot create default storage directory : "
                        + defaultStorageDir.getAbsolutePath() + "\n Checkpointing and recovery will be disabled.");
            }
        }
        return storageRootPath;

    }

}
