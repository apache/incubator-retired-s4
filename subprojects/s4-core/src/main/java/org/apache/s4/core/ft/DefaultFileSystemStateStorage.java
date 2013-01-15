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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * <p>
 * Implementation of a file system backend storage to persist checkpoints.
 * </p>
 * <p>
 * The file system may be the default local file system when running on a single machine, but should be a distributed
 * file system such as NFS when running on a cluster.
 * </p>
 * <p>
 * Checkpoints are stored in individual files (1 file = 1 checkpointId) in directories according to the following
 * structure: <code>(storageRootpath)/prototypeId/checkpointId</code>
 * </p>
 * 
 */
public class DefaultFileSystemStateStorage implements StateStorage {

    private static Logger logger = LoggerFactory.getLogger(DefaultFileSystemStateStorage.class);

    @Inject
    @Named("s4.checkpointing.filesystem.storageRootPath")
    String storageRootPath;

    public DefaultFileSystemStateStorage() {
    }

    /**
     * <p>
     * Called by the dependency injection framework, after construction.
     * <p/>
     */
    @Inject
    public void init() {
    }

    @Override
    public byte[] fetchState(CheckpointId key) {
        File file = checkpointID2File(key, storageRootPath);
        if (file != null && file.exists()) {
            logger.debug("Fetching " + file.getAbsolutePath() + "for : " + key);

            try {
                return Files.toByteArray(file);
            } catch (IOException e) {
                logger.error("Cannot read content from checkpoint file [" + file.getAbsolutePath() + "]", e);
                return null;
            }
        } else {
            return null;
        }

    }

    @Override
    public Set<CheckpointId> fetchStoredKeys() {
        Set<CheckpointId> keys = new HashSet<CheckpointId>();
        File rootDir = new File(storageRootPath);
        File[] dirs = rootDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        for (File dir : dirs) {
            File[] files = dir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return (file.isFile());
                }
            });
            for (File file : files) {
                keys.add(file2CheckpointID(file));
            }
        }
        return keys;
    }

    // files kept as : root/<prototypeId>/encodedKeyWithFullInfo
    private static File checkpointID2File(CheckpointId key, String storageRootPath) {

        return new File(storageRootPath + File.separator + key.getPrototypeId() + File.separator
                + Base64.encodeBase64URLSafeString(key.getStringRepresentation().getBytes()));
    }

    private static CheckpointId file2CheckpointID(File file) {
        CheckpointId id = null;
        id = new CheckpointId(new String(Base64.decodeBase64(file.getName())));
        return id;
    }

    @Override
    public void saveState(CheckpointId key, byte[] state, StorageCallback callback) {
        File f = checkpointID2File(key, storageRootPath);
        if (logger.isDebugEnabled()) {
            logger.debug("Checkpointing [" + key + "] into file: [" + f.getAbsolutePath() + "]");
        }
        if (!f.exists()) {
            if (!f.getParentFile().exists()) {
                // parent file has prototype id
                if (!f.getParentFile().mkdirs()) {
                    callback.storageOperationResult(CheckpointingFramework.StorageResultCode.FAILURE,
                            "Cannot create directory for storing PE [" + key.toString() + "] for prototype: "
                                    + f.getParentFile().getAbsolutePath());
                    return;
                }
            }
        } else {
            if (!f.delete()) {
                callback.storageOperationResult(CheckpointingFramework.StorageResultCode.FAILURE,
                        "Cannot delete previously saved checkpoint file [" + f.getParentFile().getAbsolutePath() + "]");
                return;
            }
        }

        try {
            Files.write(state, f);
            callback.storageOperationResult(CheckpointingFramework.StorageResultCode.SUCCESS, key.toString());
        } catch (FileNotFoundException e) {
            logger.error("Cannot write checkpoint file [" + f.getAbsolutePath() + "]", e);
            callback.storageOperationResult(CheckpointingFramework.StorageResultCode.FAILURE, key.toString() + " : "
                    + e.getMessage());
        } catch (IOException e) {
            logger.error("Cannot write checkpoint file [" + f.getAbsolutePath() + "]", e);
            callback.storageOperationResult(CheckpointingFramework.StorageResultCode.FAILURE, key.toString() + " : "
                    + e.getMessage());
        }

    }

}
