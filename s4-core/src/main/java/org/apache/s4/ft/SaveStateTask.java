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
package org.apache.s4.ft;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;


/**
 * 
 * Encapsulates a checkpoint request. It is scheduled by the checkpointing framework.
 *
 */
public class SaveStateTask implements Runnable {
    
    SafeKeeperId safeKeeperId;
    byte[] serializedState;
    Future<byte[]> futureSerializedState = null;
    StorageCallback storageCallback;
    StateStorage stateStorage;
    
    public SaveStateTask(SafeKeeperId safeKeeperId, byte[] state, StorageCallback storageCallback, StateStorage stateStorage) {
        super();
        this.safeKeeperId = safeKeeperId;
        this.serializedState = state;
        this.storageCallback = storageCallback;
        this.stateStorage = stateStorage;
    }
    
    public SaveStateTask(SafeKeeperId safeKeeperId, Future<byte[]> futureSerializedState, StorageCallback storageCallback, StateStorage stateStorage) {
    	this.safeKeeperId = safeKeeperId;
        this.futureSerializedState = futureSerializedState;
        this.storageCallback = storageCallback;
        this.stateStorage = stateStorage;
    }
    
    @Override
    public void run() {
    	if (futureSerializedState!=null) {
    		try {
				stateStorage.saveState(safeKeeperId, futureSerializedState.get(), storageCallback);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				Logger.getLogger(SaveStateTask.class).warn("Cannot save checkpoint : " + safeKeeperId, e);
			}
    	}
    }
}
