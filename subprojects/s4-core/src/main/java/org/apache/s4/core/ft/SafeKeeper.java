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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.util.S4Metrics.CheckpointingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * 
 * <p>
 * This class is responsible for coordinating interactions between the S4 event processor and the checkpoint storage
 * backend. In particular, it schedules asynchronous save tasks to be executed on the backend.
 * </p>
 * 
 * 
 * 
 */
public final class SafeKeeper implements CheckpointingFramework {

    private static final class UncaughtExceptionLogger implements UncaughtExceptionHandler {
        String operationType;

        public UncaughtExceptionLogger(String operationType) {
            this.operationType = operationType;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Cannot execute checkpointing " + operationType + " operation", e);
        }
    }

    private static Logger logger = LoggerFactory.getLogger(SafeKeeper.class);

    @Inject
    private StateStorage stateStorage;
    @Inject(optional = true)
    private StorageCallbackFactory storageCallbackFactory = new LoggingStorageCallbackFactory();

    private ThreadPoolExecutor storageThreadPool;
    private ThreadPoolExecutor serializationThreadPool;
    private ThreadPoolExecutor fetchingThreadPool;

    @Inject(optional = true)
    @Named("s4.checkpointing.storageMaxThreads")
    int storageMaxThreads = 1;

    @Inject(optional = true)
    @Named("s4.checkpointing.storageThreadKeepAliveSeconds")
    int storageThreadKeepAliveSeconds = 120;

    @Inject(optional = true)
    @Named("s4.checkpointing.storageMaxOutstandingRequests")
    int storageMaxOutstandingRequests = 1000;

    @Inject(optional = true)
    @Named("s4.checkpointing.serializationMaxThreads")
    int serializationMaxThreads = 1;

    @Inject(optional = true)
    @Named("s4.checkpointing.serializationThreadKeepAliveSeconds")
    int serializationThreadKeepAliveSeconds = 120;

    @Inject(optional = true)
    @Named("s4.checkpointing.serializationMaxOutstandingRequests")
    int serializationMaxOutstandingRequests = 1000;

    @Inject(optional = true)
    @Named("s4.checkpointing.maxSerializationLockTime")
    long maxSerializationLockTime = 1000;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingMaxThreads")
    int fetchingMaxThreads = 1;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingThreadKeepAliveSeconds")
    int fetchingThreadKeepAliveSeconds = 120;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingMaxWaitMs")
    long fetchingMaxWaitMs = 1000;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingMaxConsecutiveFailuresBeforeDisabling")
    int fetchingMaxConsecutiveFailuresBeforeDisabling = 10;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingDisabledDurationMs")
    long fetchingDisabledDurationMs = 600000;

    @Inject(optional = true)
    @Named("s4.checkpointing.fetchingQueueSize")
    int fetchingQueueSize = 100;

    long fetchingDisabledInitTime = -1;
    AtomicInteger fetchingCurrentConsecutiveFailures = new AtomicInteger();

    public SafeKeeper() {
    }

    @Inject
    private void init() {

        // NOTE: those thread pools should be fine tuned according to backend and application load/requirements.
        // For now:
        // - number of threads and work queue size have overridable defaults
        // - failures are logged
        // - when storage queue is full, we throttle backwards to the serialization threadpool
        // - when serialization queue is full, we abort execution for new entries
        // - fetching uses a synchronous queue and therefore is a blocking operation, with a timeout

        ThreadFactory storageThreadFactory = new ThreadFactoryBuilder().setNameFormat("Checkpointing-storage-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionLogger("storage")).build();
        storageThreadPool = new ThreadPoolExecutor(1, storageMaxThreads, storageThreadKeepAliveSeconds,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(storageMaxOutstandingRequests),
                storageThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        storageThreadPool.allowCoreThreadTimeOut(true);

        ThreadFactory serializationThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Checkpointing-serialization-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionLogger("serialization")).build();
        serializationThreadPool = new ThreadPoolExecutor(1, serializationMaxThreads,
                serializationThreadKeepAliveSeconds, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                        serializationMaxOutstandingRequests), serializationThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
        serializationThreadPool.allowCoreThreadTimeOut(true);

        ThreadFactory fetchingThreadFactory = new ThreadFactoryBuilder().setNameFormat("Checkpointing-fetching-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionLogger("fetching")).build();
        fetchingThreadPool = new ThreadPoolExecutor(0, fetchingMaxThreads, fetchingThreadKeepAliveSeconds,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(fetchingQueueSize), fetchingThreadFactory);
        fetchingThreadPool.allowCoreThreadTimeOut(true);

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.ft.CheckpointingFramework#saveState(org.apache.s4.core.ProcessingElement)
     */
    @Override
    public StorageCallback saveState(ProcessingElement pe) {
        StorageCallback storageCallback = storageCallbackFactory.createStorageCallback();
        Future<byte[]> futureSerializedState = null;
        try {
            futureSerializedState = serializeState(pe);
        } catch (RejectedExecutionException e) {
            CheckpointingMetrics.rejectedSerializationTask();
            storageCallback.storageOperationResult(StorageResultCode.FAILURE,
                    "Serialization task queue is full. An older serialization task was dumped in order to serialize PE ["
                            + pe.getId() + "]" + "	Remaining capacity for the serialization task queue is ["
                            + serializationThreadPool.getQueue().remainingCapacity() + "] ; number of elements is ["
                            + serializationThreadPool.getQueue().size() + "] ; maximum capacity is ["
                            + serializationThreadPool + "]");
            return storageCallback;
        }
        submitSaveStateTask(new SaveStateTask(new CheckpointId(pe), futureSerializedState, storageCallback,
                stateStorage), storageCallback);
        return storageCallback;
    }

    private Future<byte[]> serializeState(ProcessingElement pe) {
        Future<byte[]> future = serializationThreadPool.submit(new SerializeTask(pe));
        return future;
    }

    private void submitSaveStateTask(SaveStateTask task, StorageCallback storageCallback) {
        try {
            storageThreadPool.execute(task);
        } catch (RejectedExecutionException e) {
            CheckpointingMetrics.rejectedStorageTask();
            storageCallback.storageOperationResult(StorageResultCode.FAILURE,
                    "Storage checkpoint queue is full. Removed an old task to handle latest task. Remaining capacity for task queue is ["
                            + storageThreadPool.getQueue().remainingCapacity() + "] ; number of elements is ["
                            + storageThreadPool.getQueue().size() + "] ; maximum capacity is ["
                            + storageMaxOutstandingRequests + "]");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.ft.CheckpointingFramework#fetchSerializedState(org.apache.s4.core.ft.SafeKeeperId)
     */
    @Override
    public byte[] fetchSerializedState(CheckpointId key) {

        byte[] result = null;

        if (fetchingCurrentConsecutiveFailures.get() == fetchingMaxConsecutiveFailuresBeforeDisabling) {
            if ((fetchingDisabledInitTime + fetchingDisabledDurationMs) < System.currentTimeMillis()) {
                return null;
            } else {
                // reached time, reinit
                fetchingCurrentConsecutiveFailures.set(0);
            }
        }
        Future<byte[]> fetched = fetchingThreadPool.submit(new FetchTask(stateStorage, key));
        try {
            result = fetched.get(fetchingMaxWaitMs, TimeUnit.MILLISECONDS);
            CheckpointingMetrics.fetchedCheckpoint();
            fetchingCurrentConsecutiveFailures.set(0);
            return result;
        } catch (TimeoutException te) {
            logger.error("Cannot fetch checkpoint from backend for key [{}] before timeout of {} ms",
                    key.getStringRepresentation(), fetchingMaxWaitMs);
        } catch (InterruptedException e) {
            logger.error(
                    "Cannot fetch checkpoint from backend for key [{}] before timeout of {} ms because of an interruption",
                    key.getStringRepresentation(), fetchingMaxWaitMs);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Cannot fetch checkpoint from backend for key [{}] due to {}", key.getStringRepresentation(),
                    e.getCause().getClass().getName() + "/" + e.getCause().getMessage());
        }
        CheckpointingMetrics.checkpointFetchFailed();
        if (fetchingCurrentConsecutiveFailures.incrementAndGet() == fetchingMaxConsecutiveFailuresBeforeDisabling) {
            logger.trace(
                    "Due to {} successive checkpoint fetching failures, fetching is temporarily disabled for {} ms",
                    fetchingMaxConsecutiveFailuresBeforeDisabling, fetchingDisabledDurationMs);
            fetchingDisabledInitTime = System.currentTimeMillis();
        }

        return result;
    }

    @Override
    public boolean isCheckpointable(ProcessingElement pe) {
        if (pe.getCheckpointingConfig().mode.equals(CheckpointingConfig.CheckpointingMode.NONE)) {
            return false;
        }
        if (pe.getCheckpointingConfig().frequency > 0 && pe.isDirty()) {
            if (pe.getCheckpointingConfig().mode.equals(CheckpointingConfig.CheckpointingMode.EVENT_COUNT)) {
                if (pe.getEventCount() % pe.getCheckpointingConfig().frequency == 0) {
                    return true;
                }
            }
        }

        return false;
    }

}
