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

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;

public class RemoteSender {

    final private Emitter emitter;
    final private Hasher hasher;
    int targetPartition = 0;

    public RemoteSender(Emitter emitter, Hasher hasher) {
        super();
        this.emitter = emitter;
        this.hasher = hasher;
    }

    public void send(String hashKey, EventMessage eventMessage) {
        if (hashKey == null) {
            // round robin by default
            emitter.send(Math.abs(targetPartition++ % emitter.getPartitionCount()), eventMessage);
        } else {
            int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());
            emitter.send(partition, eventMessage);
        }
    }
}
