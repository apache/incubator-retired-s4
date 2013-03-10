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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;

/**
 * Sends events to a remote cluster (round-robin by default).
 * 
 */
public class RemoteSender {

    final private Emitter emitter;
    final private Hasher hasher;
    AtomicInteger targetPartition = new AtomicInteger();
    final private String remoteClusterName;

    public RemoteSender(Emitter emitter, Hasher hasher, String clusterName) {
        super();
        this.emitter = emitter;
        this.hasher = hasher;
        this.remoteClusterName = clusterName;

    }

    public void send(String hashKey, ByteBuffer message) throws InterruptedException {
        int partition;
        if (hashKey == null) {
            // round robin by default
            partition = Math.abs(targetPartition.incrementAndGet() % emitter.getPartitionCount());
        } else {
            partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());
        }
        emitter.send(partition, message);
    }
}
