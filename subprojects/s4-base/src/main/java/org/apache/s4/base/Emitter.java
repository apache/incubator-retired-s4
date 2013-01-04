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

package org.apache.s4.base;

import java.nio.ByteBuffer;

/**
 * Defines an event emitter, responsible for sending an event to a given partition of the cluster.
 * 
 */
public interface Emitter {

    /**
     * @param partitionId
     *            - destination partition
     * 
     * @param message
     *            - message payload that needs to be sent
     * 
     * @return - true - if message is sent across successfully - false - if send fails
     * @throws InterruptedException
     *             if interrupted during blocking send operation
     */
    boolean send(int partitionId, ByteBuffer message) throws InterruptedException;

    int getPartitionCount();

    void close();
}
