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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.comm.topology.PartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends events to a remote cluster.
 * 
 */
public class RemoteSender {

    private static final Logger logger = LoggerFactory.getLogger(RemoteSender.class);

    final private Emitter emitter;
    final private Hasher hasher;
    AtomicInteger targetPartition = new AtomicInteger();
    final private String remoteClusterName;
    private Map<String, PartitionData> partitionDatas;

    public RemoteSender(Emitter emitter, Hasher hasher, String clusterName) {
        super();
        this.emitter = emitter;
        this.hasher = hasher;
        this.remoteClusterName = clusterName;
    }

    public void setPartitionDatas(Map<String, PartitionData> partitionDatas) {
        this.partitionDatas = partitionDatas;
    }

    public void send(String hashKey, ByteBuffer message) throws InterruptedException {
        
        Set<Integer> partitions = new HashSet<Integer>();

        logger.warn("Remote sending with hash: " + hashKey);
        int hashValue = (hashKey == null) ? targetPartition.incrementAndGet() : (int) hasher.hash(hashKey);

        for (String prototype : partitionDatas.keySet()) {
            PartitionData data = partitionDatas.get(prototype);
            partitions.add(data.getGlobalePartitionId(hashValue % data.getPartitionCount()));
        }

        for (Integer partition : partitions) {
            logger.warn("Remote sending to partition: " + partition);
            emitter.send(partition, message);
        }
    }
}
