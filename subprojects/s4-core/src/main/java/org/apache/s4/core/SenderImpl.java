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
import java.util.concurrent.ExecutorService;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Sender;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.core.staging.SenderExecutorServiceFactory;
import org.apache.s4.core.util.S4Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * The {@link SenderImpl} and its counterpart {@link ReceiverImpl} are the top level classes of the communication layer.
 * <p>
 * {@link SenderImpl} is responsible for sending an event to a {@link ProcessingElement} instance using a hashKey.
 * <p>
 * Details on how the cluster is partitioned and how events are serialized and transmitted to its destination are hidden
 * from the application developer.
 */
public class SenderImpl implements Sender, ClusterChangeListener {

    private static Logger logger = LoggerFactory.getLogger(SenderImpl.class);

    final private Emitter emitter;
    final private SerializerDeserializer serDeser;
    final private Hasher hasher;
    private final Cluster cluster;

    Assignment assignment;
    private int localPartitionId = -1;

    private final ExecutorService tpe;

    @Inject
    S4Metrics metrics;

    /**
     * 
     * @param emitter
     *            the emitter implements the low level communication layer.
     * @param serDeser
     *            a serialization mechanism.
     * @param hasher
     *            a hashing function to map keys to partition IDs.
     * @param assignment
     *            partition assignment from the cluster manager
     * @param senderExecutorServiceFactory
     *            factory for creating sender executors
     * @param cluster
     *            cluster information
     */
    @Inject
    public SenderImpl(Emitter emitter, SerializerDeserializer serDeser, Hasher hasher, Assignment assignment,
            SenderExecutorServiceFactory senderExecutorServiceFactory, Cluster cluster) {
        this.emitter = emitter;
        this.serDeser = serDeser;
        this.hasher = hasher;
        this.assignment = assignment;
        this.tpe = senderExecutorServiceFactory.create();
        this.cluster = cluster;
    }

    @Inject
    private void init() {
        cluster.addListener(this);
        resolveLocalPartitionId();
    }

    private void resolveLocalPartitionId() {
        ClusterNode node = assignment.assignClusterNode();
        if (node != null) {
            localPartitionId = node.getPartition();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.Sender#checkAndSendIfNotLocal(java.lang.String, org.apache.s4.base.Event)
     */
    @Override
    public boolean checkAndSendIfNotLocal(String hashKey, Event event) {
        int partition = (int) (hasher.hash(hashKey) % emitter.getPartitionCount());
        if (partition == localPartitionId) {
            metrics.sentLocal();
            /* Hey we are in the same JVM, don't use the network. */
            return false;
        }
        send(partition, event);
        metrics.sentEvent(partition);
        return true;
    }

    private void send(int partition, Event event) {
        tpe.submit(new SerializeAndSendToRemotePartitionTask(event, partition));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.Sender#sendToRemotePartitions(org.apache.s4.base.Event)
     */
    @Override
    public void sendToAllRemotePartitions(Event event) {
        tpe.submit(new SerializeAndSendToAllRemotePartitionsTask(event));

    }

    class SerializeAndSendToRemotePartitionTask implements Runnable {
        Event event;
        int remotePartitionId;

        public SerializeAndSendToRemotePartitionTask(Event event, int remotePartitionId) {
            this.event = event;
            this.remotePartitionId = remotePartitionId;
        }

        @Override
        public void run() {
            ByteBuffer serializedEvent = serDeser.serialize(event);
            try {
                emitter.send(remotePartitionId, serializedEvent);
            } catch (InterruptedException e) {
                logger.error("Interrupted blocking send operation for event {}. Event is lost.", event);
                Thread.currentThread().interrupt();
            }

        }

    }

    class SerializeAndSendToAllRemotePartitionsTask implements Runnable {

        Event event;

        public SerializeAndSendToAllRemotePartitionsTask(Event event) {
            super();
            this.event = event;
        }

        @Override
        public void run() {
            ByteBuffer serializedEvent = serDeser.serialize(event);

            for (int i = 0; i < emitter.getPartitionCount(); i++) {

                /* Don't use the comm layer when we send to the same partition. */
                if (localPartitionId != i) {
                    try {
                        emitter.send(i, serializedEvent);
                        metrics.sentEvent(i);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted blocking send operation for event {}. Event is lost.", event);
                        // no reason to continue: we were interrupted, so we reset the interrupt status and leave
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

        }

    }

    @Override
    public void onChange() {
        resolveLocalPartitionId();
    }

}
