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

import static org.apache.s4.util.MetricsName.S4_CORE_METRICS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.s4.dispatcher.Dispatcher;
import org.apache.s4.dispatcher.partitioner.Hasher;
import org.apache.s4.emitter.CommLayerEmitter;
import org.apache.s4.logger.Monitor;
import org.apache.s4.processor.AbstractPE;
import org.apache.s4.serialize.SerializerDeserializer;
import org.apache.s4.util.MetricsName;

/**
 *
 * <p>
 * This class is responsible for coordinating interactions between the S4 event
 * processor and the checkpoint storage backend. In particular, it schedules
 * asynchronous save tasks to be executed on the backend.
 * </p>
 *
 *
 *
 */
public class SafeKeeper {

    public enum StorageResultCode {
        SUCCESS, FAILURE
    }

    private static Logger logger = Logger.getLogger("s4-ft");
    private StateStorage stateStorage;
    private Dispatcher loopbackDispatcher;
    private SerializerDeserializer serializer;
    private Hasher hasher;
    // monitor field injection through a latch
    private CountDownLatch signalReady = new CountDownLatch(2);
    private CountDownLatch signalNodesAvailable = new CountDownLatch(1);
    private StorageCallbackFactory storageCallbackFactory = new LoggingStorageCallbackFactory();

    private ThreadPoolExecutor storageThreadPool;
    private ThreadPoolExecutor serializationThreadPool;
    private ThreadPoolExecutor fetchingThreadPool;

    private CheckpointingCoordinator processingSerializationSynchro;

    Monitor monitor;

    int storageMaxThreads = 1;
    int storageThreadKeepAliveSeconds = 120;
    int storageMaxOutstandingRequests = 1000;

    int serializationThreadKeepAliveSeconds = 120;
    int serializationMaxOutstandingRequests = 1000;

    long fetchingMaxWaitMs = 1000;
    int fetchingMaxConsecutiveFailuresBeforeDisabling = 10;
    int fetchingCurrentConsecutiveFailures = 0;
    long fetchingDisabledDurationMs = 600000;
    long fetchingDisabledInitTime=-1;


    long maxSerializationLockTime = 1000;

    public SafeKeeper() {
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }
    /**
     * <p>
     * This init() method <b>must</b> be called by the dependency injection
     * framework. It waits until all required dependencies are injected in
     * SafeKeeper, and until the node count is accessible from the communication
     * layer.
     * </p>
     */
    public void init() {
        try {
            getReadySignal().await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        storageThreadPool = new ThreadPoolExecutor(1, storageMaxThreads, storageThreadKeepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(storageMaxOutstandingRequests));
        serializationThreadPool = new ThreadPoolExecutor(1, 1, serializationThreadKeepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(serializationMaxOutstandingRequests));
        fetchingThreadPool = new ThreadPoolExecutor(1, 1, serializationThreadKeepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(serializationMaxOutstandingRequests));

        processingSerializationSynchro = new CheckpointingCoordinator(maxSerializationLockTime);

        logger.debug("Started thread pool with maxWriteThreads=[" + storageMaxThreads
                + "], writeThreadKeepAliveSeconds=[" + storageThreadKeepAliveSeconds + "], maxOutsandingWriteRequests=["
                + storageMaxOutstandingRequests + "]");

        int nodeCount = getLoopbackDispatcher().getEventEmitter().getNodeCount();
        // required wait until nodes are available
        while (nodeCount == 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            nodeCount = getLoopbackDispatcher().getEventEmitter().getNodeCount();
        }

        signalNodesAvailable.countDown();
    }

    /**
     * Synchronization to prevent race conditions with serialization threads
     */
    public void acquirePermitForProcessing(AbstractPE pe) {
        processingSerializationSynchro.acquireForProcessing(pe);
    }

    /**
     * Notification part of the mechanism for preventing race condition with serialization threads
     */
    public void releasePermitForProcessing(AbstractPE pe) {
        processingSerializationSynchro.releaseFromProcessing(pe);
    }


    /**
     * Serializes and stores state to the storage backend. Serialization and storage operations are asynchronous.
     *
     * @return a callback for getting notified of the result of the storage operation
     */
    public StorageCallback saveState(AbstractPE pe) {
        StorageCallback storageCallback = storageCallbackFactory.createStorageCallback();
        Future<byte[]> futureSerializedState = null;
        try {
            futureSerializedState = serializeState(pe, processingSerializationSynchro);
        } catch (RejectedExecutionException e) {
            if (monitor!=null) {
                monitor.increment(MetricsName.checkpointing_dropped_from_serialization_queue.toString(), 1, S4_CORE_METRICS.toString());
            }
            storageCallback.storageOperationResult(StorageResultCode.FAILURE,
                    "Serialization task queue is full. An older serialization task was dumped in order to serialize PE ["+ pe.getId()+"]" +
                            "    Remaining capacity for the serialization task queue is ["
                            + serializationThreadPool.getQueue().remainingCapacity() + "] ; number of elements is ["
                            + serializationThreadPool.getQueue().size() + "] ; maximum capacity is [" + serializationThreadPool
                            + "]");
            return storageCallback;
        }
        submitSaveStateTask(new SaveStateTask(pe.getSafeKeeperId(), futureSerializedState, storageCallback, stateStorage), storageCallback);
        return storageCallback;
    }

    private Future<byte[]> serializeState(AbstractPE pe, CheckpointingCoordinator coordinator) {
        Future<byte[]> future = serializationThreadPool.submit(new SerializeTask(pe, coordinator));
        if(monitor!=null) {
            monitor.increment(MetricsName.checkpointing_added_to_serialization_queue.toString(), 1, S4_CORE_METRICS.toString());
        }
        return future;
    }

    private void submitSaveStateTask(SaveStateTask task, StorageCallback storageCallback) {
        try {
            storageThreadPool.execute(task);
            if (monitor!=null) {
                monitor.increment(MetricsName.checkpointing_added_to_storage_queue.toString(), 1, S4_CORE_METRICS.toString());
            }
        } catch (RejectedExecutionException e) {
            if (monitor!=null) {
                monitor.increment(MetricsName.checkpointing_dropped_from_storage_queue.toString(), 1, S4_CORE_METRICS.toString());
            }
            storageCallback.storageOperationResult(StorageResultCode.FAILURE,
                    "Storage checkpoint queue is full. Removed an old task to handle latest task. Remaining capacity for task queue is ["
                            + storageThreadPool.getQueue().remainingCapacity() + "] ; number of elements is ["
                            + storageThreadPool.getQueue().size() + "] ; maximum capacity is [" + storageMaxOutstandingRequests
                            + "]");
        }
    }

    /**
     * Fetches checkpoint data from storage for a given PE
     *
     * @param key
     *            safeKeeperId
     * @return checkpoint data
     */
    public byte[] fetchSerializedState(SafeKeeperId key) {

        try {
            signalNodesAvailable.await();
        } catch (InterruptedException ignored) {
        }
        byte[] result = null;
        if ((fetchingCurrentConsecutiveFailures>0 && (fetchingCurrentConsecutiveFailures== fetchingMaxConsecutiveFailuresBeforeDisabling))) {
            if((fetchingDisabledInitTime+fetchingDisabledDurationMs)<System.currentTimeMillis()) {
                return null;
            } else {
                // reached time, reinit
                fetchingCurrentConsecutiveFailures=0;
            }
        }
        Future<byte[]> fetched = fetchingThreadPool.submit(new FetchTask(stateStorage, this, key));
        try {
            result = fetched.get(fetchingMaxWaitMs, TimeUnit.MILLISECONDS);
            fetchingCurrentConsecutiveFailures=0;
        } catch (Exception e) {
            logger.error("Cannot fetch checkpoint from backend for key ["+ key.getStringRepresentation()+"]", e);
            fetchingCurrentConsecutiveFailures++;
            if (fetchingCurrentConsecutiveFailures==fetchingMaxConsecutiveFailuresBeforeDisabling) {
                fetchingDisabledInitTime = System.currentTimeMillis();
            }
        }
        return result;
    }

    /**
     * Generates a recovery event, and enqueues it in the local event queue.<br/>
     * This can be used for an eager recovery mechanism.
     *
     * @param safeKeeperId
     *            safeKeeperId to recover
     */
    public void initiateRecovery(SafeKeeperId safeKeeperId) {
        RecoveryEvent recoveryEvent = new RecoveryEvent(safeKeeperId);
        loopbackDispatcher.dispatchEvent(safeKeeperId.getPrototypeId() + "_recovery", recoveryEvent);
    }

    public void setSerializer(SerializerDeserializer serializer) {
        this.serializer = serializer;
    }

    public SerializerDeserializer getSerializer() {
        return serializer;
    }

    public int getPartitionId() {
        return ((CommLayerEmitter) loopbackDispatcher.getEventEmitter()).getListener().getId();
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
        signalReady.countDown();
    }

    public Hasher getHasher() {
        return hasher;
    }

    public void setStateStorage(StateStorage stateStorage) {
        this.stateStorage = stateStorage;
    }

    public StateStorage getStateStorage() {
        return stateStorage;
    }

    public void setLoopbackDispatcher(Dispatcher dispatcher) {
        this.loopbackDispatcher = dispatcher;
        signalReady.countDown();
    }

    public Dispatcher getLoopbackDispatcher() {
        return this.loopbackDispatcher;
    }

    public CountDownLatch getReadySignal() {
        return signalReady;
    }

    public StorageCallbackFactory getStorageCallbackFactory() {
        return storageCallbackFactory;
    }

    public void setStorageCallbackFactory(StorageCallbackFactory storageCallbackFactory) {
        this.storageCallbackFactory = storageCallbackFactory;
    }

    public int getStorageMaxThreads() {
        return storageMaxThreads;
    }

    public void setStorageMaxThreads(int storageMaxThreads) {
        this.storageMaxThreads = storageMaxThreads;
    }

    public int getStorageThreadKeepAliveSeconds() {
        return storageThreadKeepAliveSeconds;
    }

    public void setStorageThreadKeepAliveSeconds(int storageThreadKeepAliveSeconds) {
        this.storageThreadKeepAliveSeconds = storageThreadKeepAliveSeconds;
    }

    public int getStorageMaxOutstandingRequests() {
        return storageMaxOutstandingRequests;
    }

    public void setStorageMaxOutstandingRequests(int storageMaxOutstandingRequests) {
        this.storageMaxOutstandingRequests = storageMaxOutstandingRequests;
    }

    public int getSerializationThreadKeepAliveSeconds() {
        return serializationThreadKeepAliveSeconds;
    }

    public void setSerializationThreadKeepAliveSeconds(
            int serializationThreadKeepAliveSeconds) {
        this.serializationThreadKeepAliveSeconds = serializationThreadKeepAliveSeconds;
    }

    public int getSerializationMaxOutstandingRequests() {
        return serializationMaxOutstandingRequests;
    }

    public void setSerializationMaxOutstandingRequests(
            int serializationMaxOutstandingRequests) {
        this.serializationMaxOutstandingRequests = serializationMaxOutstandingRequests;
    }

    public long getMaxSerializationLockTime() {
        return maxSerializationLockTime;
    }

    public void setMaxSerializationLockTime(long maxSerializationLockTime) {
        this.maxSerializationLockTime = maxSerializationLockTime;
    }

}
