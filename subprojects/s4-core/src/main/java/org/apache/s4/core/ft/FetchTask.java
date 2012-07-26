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

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a checkpoint fetching operation.
 *
 */
public class FetchTask implements Callable<byte[]> {

    private static Logger logger = LoggerFactory.getLogger(FetchTask.class);

    StateStorage stateStorage;
    CheckpointId checkpointId;

    public FetchTask(StateStorage stateStorage, CheckpointId checkpointId) {
        super();
        this.stateStorage = stateStorage;
        this.checkpointId = checkpointId;
    }

    @Override
    public byte[] call() throws Exception {
        try {
            byte[] result = stateStorage.fetchState(checkpointId);
            return result;
        } catch (Exception e) {
            logger.error("Cannot fetch checkpoint data for {}", checkpointId, e);
            throw e;
        }
    }

}
