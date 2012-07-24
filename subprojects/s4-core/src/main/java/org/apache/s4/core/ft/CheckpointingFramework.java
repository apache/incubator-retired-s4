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

import org.apache.s4.core.ProcessingElement;

import com.google.inject.ImplementedBy;

/**
 *
 * This interface defines the functionalities offered by the checkpointing framework.
 *
 */
@ImplementedBy(value = NoOpCheckpointingFramework.class)
public interface CheckpointingFramework {

    /**
     * Serializes and stores state to the storage backend. Serialization and storage operations are asynchronous.
     *
     * @return a callback for getting notified of the result of the storage operation
     */
    StorageCallback saveState(ProcessingElement pe);

    /**
     * Fetches checkpoint data from storage for a given PE
     *
     * @param key
     *            safeKeeperId
     * @return checkpoint data
     */
    byte[] fetchSerializedState(CheckpointId key);

    /**
     * Evaluates whether specified PE should be checkpointed, based on:
     * <ul>
     * <li>whether checkpointing enabled</li>
     * <li>whether the pe is "dirty"</li>
     * <li>the checkpointing frequency settings</li>
     * </ul>
     *
     * This is used for count-based checkpointing intervals. Time-based checkpointing relies on the dirty flag when
     * triggered.
     *
     * @param pe
     *            processing element to evaluate
     * @return true if checkpointable, according to the above requirements
     */
    boolean isCheckpointable(ProcessingElement pe);

    public enum StorageResultCode {
        SUCCESS, FAILURE
    }

}
