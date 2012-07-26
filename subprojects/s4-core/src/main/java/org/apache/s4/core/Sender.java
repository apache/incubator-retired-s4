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
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * The {@link Sender} and its counterpart {@link Receiver} are the top level classes of the communication layer.
 * <p>
 * {@link Sender} is responsible for sending an event to a {@link ProcessingElement} instance using a hashKey.
 * <p>
 * Details on how the cluster is partitioned and how events are serialized and transmitted to its destination are hidden
 * from the application developer.
 */
public class Sender {

    private static Logger logger = LoggerFactory.getLogger(Sender.class);

    final private Emitter emitter;
    final private SerializerDeserializer serDeser;
    final private Hasher hasher;

    Assignment assignment;
    private int localPartitionId = -1;

    /**
     * 
     * @param emitter
     *            the emitter implements the low level communication layer.
     * @param serDeser
     *            a serialization mechanism.
     * @param hasher
     *            a hashing function to map keys to partition IDs.
     */
    @Inject
    public Sender(Emitter emitter, SerializerDeserializer serDeser, Hasher hasher, Assignment assignment) {
        this.emitter = emitter;
        this.serDeser = serDeser;
        this.hasher = hasher;
        this.assignment = assignment;
    }

    @Inject
    private void resolveLocalPartitionId() {
        ClusterNode node = assignment.assignClusterNode();
        if (node != null) {
            localPartitionId = node.getPartition();
        }
    }

    /**
     * This method attempts to send an event to a remote partition. If the destination is local, the method does not
     * send the event and returns false. <b>The caller is then expected to put the event in a local queue instead.</b>
     * 
     * @param hashKey
     *            the string used to map the value of a key to a specific partition.
     * @param event
     *            the event to be delivered to a {@link ProcessingElement} instance.
     * @return true if the event was sent because the destination is <b>not</b> local.
     * 
     */
    public boolean checkAndSendIfNotLocal(String hashKey, Event event) {
        int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());

        if (partition == localPartitionId) {
            /* Hey we are in the same JVM, don't use the network. */
            return false;
        }
        send(partition,
                new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), serDeser.serialize(event)));
        return true;
    }

    private void send(int partition, EventMessage event) {
        emitter.send(partition, event);
    }

    /**
     * Send an event to all the remote partitions in the cluster. The caller is expected to also put the event in a
     * local queue.
     * 
     * @param event
     *            the event to be delivered to {@link ProcessingElement} instances.
     */
    public void sendToRemotePartitions(Event event) {

        for (int i = 0; i < emitter.getPartitionCount(); i++) {

            /* Don't use the comm layer when we send to the same partition. */
            if (localPartitionId != i)
                emitter.send(
                        i,
                        new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), serDeser
                                .serialize(event)));
        }
    }

}
