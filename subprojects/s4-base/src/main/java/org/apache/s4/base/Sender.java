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

/**
 * Defines the exit point from the event processing layer to the communication layer. The implementation is responsible
 * for serializing events and passing serialized data to the communication layer.
 * 
 */
public interface Sender {

    /**
     * This method attempts to send an event to a remote partition. If the destination is local, the method does not
     * send the event and returns false. <b>The caller is then expected to put the event in a local queue instead.</b>
     * 
     * @param hashKey
     *            the string used to map the value of a key to a specific partition.
     * @param event
     *            the event to be delivered to a Processing Element instance.
     * @return true if the event was sent because the destination is <b>not</b> local.
     * 
     */
    boolean checkAndSendIfNotLocal(String hashKey, Event event);

    /**
     * Send an event to all the remote partitions in the cluster. The caller is expected to also put the event in a
     * local queue.
     * 
     * @param event
     *            the event to be delivered to Processing Element instances.
     */
    void sendToAllRemotePartitions(Event event);

}
