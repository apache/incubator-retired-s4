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
 * Defines the entry point from the communication layer to the application layer.
 * 
 * Events received as raw bytes through the {@link Listener} implementation use the {@link Receiver#receive(ByteBuffer)}
 * method so that events can be deserialized (conversion from byte[] to Event objects) and enqueued for processing.
 * 
 */
public interface Receiver {

    /**
     * Handle a serialized message, i.e. deserialize the message and pass it to event processors.
     */
    void receive(ByteBuffer message);

    /**
     * Returns the partition id currently assigned to this node.
     */
    int getPartitionId();

}
