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

import java.util.Map;
import java.util.TimerTask;

import org.apache.s4.core.ProcessingElement;

/**
 * When checkpointing at regular time intervals, this class is used to actually perform the checkpoints. It iterates
 * among all instances of the specified prototype, and checkpoints every eligible instance.
 *
 */
public class CheckpointingTask extends TimerTask {

    ProcessingElement prototype;

    public CheckpointingTask(ProcessingElement prototype) {
        super();
        this.prototype = prototype;
    }

    @Override
    public void run() {
        Map<String, ProcessingElement> peInstances = prototype.getPEInstances();
        for (Map.Entry<String, ProcessingElement> entry : peInstances.entrySet()) {
            synchronized (entry.getValue()) {
                if (entry.getValue().isDirty()) {
                    entry.getValue().checkpoint();
                }
            }
        }
    }
}
