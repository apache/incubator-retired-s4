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

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.apache.s4.base.Event;
import org.apache.s4.core.ft.CheckpointId;
import org.apache.s4.core.ft.CheckpointingConfig;
import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;
import org.apache.s4.core.ft.CheckpointingTask;
import org.apache.s4.core.gen.OverloadDispatcher;
import org.apache.s4.core.gen.OverloadDispatcherGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * <p>
 * Base class for implementing processing in S4. All instances are organized as follows:
 * <ul>
 * <li>A PE prototype is a special type of instance that, along with {@link Stream} defines the topology of the
 * application graph.
 * <li>PE prototypes manage the creation and destruction of PE instances.
 * <li>All PE instances are clones of a PE prototype.
 * <li>PE instances are associated with a unique key.
 * <li>PE instances do the actual work by processing any number of input events of various types and emit output events
 * of various types.
 * <li>To process events, {@code ProcessingElement} dynamically matches an event type to a processing method. See
 * {@link org.apache.s4.core.gen.OverloadDispatcher} . There are two types of processing methods:
 * <ul>
 * <li>{@code onEvent(SomeEvent event)} When implemented, input events of type {@code SomeEvent} will be dispatched to
 * this method.
 * <li>{@code onTrigger(AnotherEvent event)} When implemented, input events of type {@code AnotherEvent} will be
 * dispatched to this method when certain conditions are met. See {@link #setTrigger(Class, int, long, TimeUnit)}.
 * </ul>
 * <li>
 * A PE implementation must not create threads. A periodic task can be implemented by overloading the {@link #onTime()}
 * method. See {@link #setTimerInterval(long, TimeUnit)}
 * <li>If a reference in the PE prototype shared by the PE instances, the object must be thread safe.
 * <li>The code in a PE instance is synchronized by the framework to avoid concurrency problems.
 * <li>In some special cases, it may be desirable to allow concurrency in the PE instance. For example, there may be
 * several event processing methods that can safely run concurrently. To enable concurrency, annotate the implementation
 * of {@code ProcessingElement} with {@link ThreadSafe}.
 * <li>PE instances never use the constructor. They must be initialized by implementing the {@link #onCreate()} method.
 * <li>PE class fields are cloned from the prototype. References are also copied which means that if the prototype
 * creates a collection object, all instances will be sharing the same collection object which is usually <em>NOT</em>
 * what the programmer intended . The application developer is responsible for initializing objects in the
 * {@link #onCreate()} method. For example, if each instance requires a
 * <tt>List<tt/> object the PE should implement the following:
 *         <pre>
 *         public class MyPE extends ProcessingElement {
 * 
 *           private Map<String, Integer> wordCount;
 * 
 *           ...
 * 
 *           onCreate() {
 *               wordCount = new HashMap<String, Integer>;
 *               logger.trace("Created a map for instance PE with id {}, getId());
 *           }
 *         }
 *         </pre>
 * 
 * 
 * </ul>
 * 
 * 
 * 
 * 
 */
public abstract class ProcessingElement implements Cloneable {

    transient private static final Logger logger = LoggerFactory.getLogger(ProcessingElement.class);
    transient private static final String SINGLETON = "singleton";

    transient protected App app;

    /*
     * This maps holds all the instances. We make it package private to prevent concrete classes from updating the
     * collection.
     */
    transient LoadingCache<String, ProcessingElement> peInstances;

    /* This map is initialized in the prototype and cloned to instances. */
    transient Map<Class<? extends Event>, Trigger> triggers;

    /* PE instance id. */
    protected String id = "";

    /* Private fields. */
    transient private ProcessingElement pePrototype;
    transient private boolean haveTriggers = false;
    transient private long timerIntervalInMilliseconds = 0;
    transient private ScheduledExecutorService triggerTimer;
    transient private ScheduledExecutorService checkpointingTimer;
    transient private boolean isPrototype = true;
    transient private boolean isThreadSafe = false;
    transient private String name = null;
    transient private boolean isSingleton = false;
    transient long eventCount = 0;

    transient private OverloadDispatcher overloadDispatcher;
    transient private boolean recoveryAttempted = false;
    transient private boolean dirty = false;

    transient private Timer processingTimer;

    transient private CheckpointingConfig checkpointingConfig = new CheckpointingConfig.Builder(CheckpointingMode.NONE)
            .build();

    protected ProcessingElement() {
        OverloadDispatcherGenerator oldg = new OverloadDispatcherGenerator(this.getClass());
        Class<?> overloadDispatcherClass = oldg.generate();
        try {
            overloadDispatcher = (OverloadDispatcher) overloadDispatcherClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        peInstances = CacheBuilder.newBuilder().build(new CacheLoader<String, ProcessingElement>() {
            @Override
            public ProcessingElement load(String key) throws Exception {
                return createPE(key);
            }
        });
        triggers = new MapMaker().makeMap();

        /*
         * Only the PE Prototype uses the constructor. The PEPrototype field will be cloned by the instances and point
         * to the prototype.
         */
        this.pePrototype = this;

    }

    /**
     * Create a PE prototype. By default, PE instances will never expire. Use {@code #configurePECache} to configure.
     * 
     * @param app
     *            the app that contains this PE
     */
    public ProcessingElement(App app) {
        this();
        setApp(app);
        if (app.measurePEProcessingTime) {
            processingTimer = Metrics.newTimer(getClass(), getClass().getName() + "-pe-processing-time");
        }

    }

    /**
     * This method is called by the PE timer. By default it is synchronized with the onEvent() and onTrigger() methods.
     * To execute concurrently with other methods, the {@link ProcessingElement} subclass must be annotated with
     * {@link ThreadSafe}.
     * 
     * Override this method to implement a periodic process.
     */
    protected void onTime() {
    }

    /**
     * This method is called after a PE instance is created. Use it to initialize fields that are PE instance specific.
     * PE instances are created using {#clone()}.
     * 
     * <p>
     * <b>Fields initialized in the class constructor are shared by all PE instances.</b>
     * </p>
     */
    abstract protected void onCreate();

    /**
     * This method is called before a PE instance is removed. Use it to close resources and clean up.
     */
    abstract protected void onRemove();

    /**
     * PE objects must be associated with one and only one {@code App} object.
     * 
     * @return the app
     */
    public App getApp() {
        return app;
    }

    public void setApp(App app) {
        if (this.app != null) {
            throw new RuntimeException("Application was already assigne to this processing element");
        }
        this.app = app;
        app.addPEPrototype(this, null);

    }

    /**
     * Returns the approximate number of PE instances from the cache.
     * 
     * @return the approximate number of PE instances.
     */
    public long getNumPEInstances() {

        return peInstances.size();
    }

    public Map<String, ProcessingElement> getPEInstances() {
        return peInstances.asMap();
    }

    /**
     * Set PE expiration and cache size.
     * <p>
     * PE instances will be automatically removed from the cache once a fixed duration has elapsed after the PEs
     * creation, or last access.
     * <p>
     * Least accessed PEs will automatically be removed from the cache when the number of PEs approaches maximumSize.
     * <p>
     * When this method is called all existing PE instances are destroyed.
     * 
     * 
     * @param maximumSize
     *            the approximate maximum number of PEs in the cache.
     * @param duration
     *            the PE duration
     * @param timeUnit
     *            the time unit
     * @return the PE prototype
     */
    public ProcessingElement setPECache(int maximumSize, long duration, TimeUnit timeUnit) {

        Preconditions.checkArgument(isPrototype, "This method can only be used on the PE prototype. Trigger not set.");

        peInstances = CacheBuilder.newBuilder().expireAfterAccess(duration, timeUnit).maximumSize(maximumSize)
                .build(new CacheLoader<String, ProcessingElement>() {
                    @Override
                    public ProcessingElement load(String key) throws Exception {
                        return createPE(key);
                    }
                });

        return this;
    }

    /**
     * Sets the max size of the PE cache.
     * 
     * <p>
     * Least accessed PEs will automatically be removed from the cache when the number of PEs approaches maximumSize.
     * <p>
     * When this method is called all existing PE instances are destroyed.
     * 
     * 
     * @param maximumSize
     *            the approximate maximum number of PEs in the cache.
     * @return the PE prototype
     */
    public ProcessingElement setPECache(int maximumSize) {

        Preconditions.checkArgument(isPrototype, "This method can only be used on the PE prototype. Trigger not set.");

        peInstances = CacheBuilder.newBuilder().maximumSize(maximumSize)
                .build(new CacheLoader<String, ProcessingElement>() {
                    @Override
                    public ProcessingElement load(String key) throws Exception {
                        return createPE(key);
                    }
                });

        return this;
    }

    /**
     * This trigger is fired when the following conditions occur:
     * 
     * <ul>
     * <li>An event of eventType arrived to the PE instance
     * <li>numEvents have arrived since the last time this trigger was fired -OR- time since last event is greater than
     * interval.
     * </ul>
     * 
     * <p>
     * When the trigger fires, the method <tt>trigger(EventType event)</tt> is called. Where <tt>EventType</tt> matches
     * the argument eventType.
     * 
     * @param eventType
     *            the type of event on which this trigger will fire.
     * @param numEvents
     *            number of events since last trigger activation. Must be greater than zero. (Set to one to trigger on
     *            every input event.)
     * @param interval
     *            minimum time between triggers. Set to zero if no time interval needed.
     * @param timeUnit
     *            the TimeUnit for the argument interval. Can set to null if no time interval needed.
     * @return the PE prototype
     */
    public ProcessingElement setTrigger(Class<? extends Event> eventType, int numEvents, long interval,
            TimeUnit timeUnit) {

        Preconditions.checkArgument(isPrototype, "This method can only be used on the PE prototype. Trigger not set.");
        Preconditions.checkNotNull(eventType, "Need eventType to set trigger.");
        Preconditions.checkArgument(numEvents > 0 || interval > 0,
                "To set trigger numEvent OR interval must be greater than zero.");
        Preconditions.checkArgument(timeUnit != null || interval < 1,
                "To set trigger timeUnit is needed when interval is greater than zero.");

        /* Skip trigger checking overhead if there are no triggers. */
        haveTriggers = true;

        if (timeUnit != null && timeUnit != TimeUnit.MILLISECONDS) {
            interval = timeUnit.convert(interval, TimeUnit.MILLISECONDS);
        }

        Trigger config = new Trigger(numEvents, interval);

        triggers.put(eventType, config);

        return this;
    }

    /**
     * @return the isSingleton
     */
    public boolean isSingleton() {
        return isSingleton;
    }

    /**
     * Makes this PE a singleton. A single PE instance is eagerly created and ready to receive events.
     * 
     * @param isSingleton
     * @throws ExecutionException
     */
    public ProcessingElement setSingleton(boolean isSingleton) {

        if (!isPrototype) {
            logger.warn("This method can only be used on the PE prototype.");
            return this;
        }
        this.isSingleton = isSingleton;

        return this;
    }

    /**
     * The duration of the periodic task controlled by the embedded timer.
     * 
     * @param timeUnit
     *            the timeUnt of the returned value.
     * @return the timer interval.
     */
    public long getTimerInterval(TimeUnit timeUnit) {
        return timeUnit.convert(timerIntervalInMilliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * Set a timer that calls {@link #onTime()}.
     * 
     * If {@code interval==0} the timer is disabled.
     * 
     * @param interval
     *            in timeUnit
     * @param timeUnit
     *            the timeUnit of interval
     */
    public ProcessingElement setTimerInterval(long interval, TimeUnit timeUnit) {
        timerIntervalInMilliseconds = TimeUnit.MILLISECONDS.convert(interval, timeUnit);

        Preconditions.checkArgument(isPrototype, "This method can only be used on the PE prototype. Trigger not set.");

        if (triggerTimer != null) {
            triggerTimer.shutdownNow();
        }

        if (interval == 0) {
            return this;
        }

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        logger.error("Expection from timer thread", e);
                    }
                }).setNameFormat("Timer-" + getClass().getSimpleName()).build();
        triggerTimer = Executors.newSingleThreadScheduledExecutor(threadFactory);
        return this;
    }

    /**
     * Set to true if the concrete PE class has the {@link ThreadSafe} annotation. The default is false (no annotation).
     * In general, application developers don't need to worry about thread safety in the concrete PEs. In some cases the
     * PE needs to be thread safe to avoid deadlocks. For example , if the application graph has cycles and the queues
     * are allowed to block, then some critical PEs with multiple incoming streams need to be made thread safe to avoid
     * locking the entire PE instance.
     * 
     * @return true if the PE implementation is considered thread safe.
     */
    public boolean isThreadSafe() {
        return isThreadSafe;
    }

    protected void handleInputEvent(Event event) {
        TimerContext timerContext = null;
        if (processingTimer != null) {
            // if timing enabled
            timerContext = processingTimer.time();
        }
        Object object;
        if (isThreadSafe) {
            object = new Object(); // a dummy object TODO improve this.
        } else {
            object = this;
        }
        synchronized (object) {
            if (!recoveryAttempted) {
                recover();
                recoveryAttempted = true;
            }

            /* Dispatch onEvent() method. */
            overloadDispatcher.dispatchEvent(this, event);

            /* Dispatch onTrigger() method. */
            if (haveTriggers && isTrigger(event)) {
                overloadDispatcher.dispatchTrigger(this, event);
            }

            eventCount++;

            dirty = true;

            if (isCheckpointable()) {
                checkpoint();
            }
        }
        if (timerContext != null) {
            // if timing enabled
            timerContext.stop();
        }
    }

    protected boolean isCheckpointable() {
        return getApp().checkpointingFramework.isCheckpointable(this);
    }

    public void checkpoint() {
        getApp().getCheckpointingFramework().saveState(this);
        clearDirty();
    }

    private boolean isTrigger(Event event) {
        return isTrigger(event, event.getClass());
    }

    /**
     * Checks the trigger for this event type. Creates an inactive trigger if no trigger is found after recursively
     * exploring the event class hierarchy. An inactive trigger never triggers.
     * 
     * @return true if trigger is reached, false if trigger is not ready yet or if trigger is inactive
     * 
     */
    private boolean isTrigger(Event event, Class<?> triggerClass) {
        /* Check if there is a trigger for this event type. Create an */
        Trigger trigger = triggers.get(triggerClass);

        if (trigger == null) {
            if (!Event.class.isAssignableFrom(triggerClass)) {
                // reached termination condition
                triggers.put(event.getClass(), new Trigger());
                return false;
            } else {
                // further explore hierarchy
                return isTrigger(event, triggerClass.getSuperclass());
            }
        } else {
            /*
             * Check if it is time to activate the trigger for this event type.
             */
            return trigger.checkAndUpdate();
        }
    }

    private void removeInstanceForKeyInternal(String id) {

        if (id == null)
            return;

        /* First let the PE instance clean after itself. */
        onRemove();

        /* Remove PE instance. */
        peInstances.invalidate(id);
    }

    protected void removeAll() {

        /* Close resources in prototype. */
        if (triggerTimer != null) {
            triggerTimer.shutdownNow();
            logger.info("Trigger timer stopped.");
        }

        /* Remove all the instances. */
        peInstances.invalidateAll();
    }

    protected void close() {
        removeInstanceForKeyInternal(id);
    }

    private ProcessingElement createPE(String id) {
        ProcessingElement pe = (ProcessingElement) this.clone();
        pe.isPrototype = false;
        pe.id = id;
        pe.triggers = Maps.newHashMap(triggers);
        pe.onCreate();
        logger.trace("Num PE instances: {}.", getNumPEInstances());
        return pe;
    }

    /* This method is called by App just before the application starts. */
    protected void initPEPrototypeInternal() {

        /* Eagerly create singleton PE. */
        if (isSingleton) {
            try {
                peInstances.get(SINGLETON);
                logger.trace("Created singleton [{}].", getInstanceForKey(SINGLETON));
            } catch (ExecutionException e) {
                logger.error("Problem when trying to create a PE instance.", e);
            }
        }

        /* Start timer. */
        if (triggerTimer != null) {
            triggerTimer.scheduleAtFixedRate(new OnTimeTask(), 0, timerIntervalInMilliseconds, TimeUnit.MILLISECONDS);
            logger.debug("Started timer for PE prototype [{}], ID [{}] with interval [{}].", new String[] {
                    this.getClass().getName(), id, String.valueOf(timerIntervalInMilliseconds) });
        }

        if (checkpointingConfig.mode == CheckpointingMode.TIME) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                    .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            logger.error("Expection from checkpointing thread", e);
                        }
                    }).setNameFormat("Checkpointing-trigger-" + getClass().getSimpleName()).build();
            checkpointingTimer = Executors.newSingleThreadScheduledExecutor(threadFactory);
            checkpointingTimer.scheduleAtFixedRate(new CheckpointingTask(this), checkpointingConfig.frequency,
                    checkpointingConfig.frequency, checkpointingConfig.timeUnit);
            logger.debug(
                    "Started checkpointing timer for PE prototype [{}], ID [{}] with interval [{}] [{}].",
                    new String[] { this.getClass().getName(), id, String.valueOf(checkpointingConfig.frequency),
                            String.valueOf(checkpointingConfig.timeUnit.toString()) });
        }

        /* Check if this PE is annotated as thread safe. */
        if (getClass().isAnnotationPresent(ThreadSafe.class) == true) {

            // TODO: this doesn't seem to be working. isannotationpresent always returns false.

            isThreadSafe = true;
            logger.trace("Annotated with @ThreadSafe");
        }

    }

    /**
     * This method is designed to be used within the package. We make it package-private. The returned instances are all
     * in the same JVM. Do not use it to access remote objects.
     * 
     * @throws ExecutionException
     */
    public ProcessingElement getInstanceForKey(String id) {

        /* Check if instance for key exists, otherwise create one. */
        try {
            if (isSingleton) {
                return peInstances.get(SINGLETON);
            }
            return peInstances.get(id);
        } catch (ExecutionException e) {
            logger.error("Problem when trying to create a PE instance for id {}", id, e);
        }
        return null;
    }

    /**
     * Get all the local instances. See notes in {@link #getInstanceForKey(String) getLocalInstanceForKey}
     */
    public Collection<ProcessingElement> getInstances() {
        try {
            if (isSingleton) {
                return ImmutableList.of(peInstances.get(SINGLETON));
            } else {
                return peInstances.asMap().values();
            }
        } catch (ExecutionException e) {
            logger.error("Problem when trying to create a PE instance for id {}", id, e);
            return null;
        }
    }

    /**
     * This method returns a remote PE instance for key. TODO: not implemented for cluster configuration yet, use it
     * only in single node configuration. for testing apps.
     * 
     * @return pe instance for key. Null if if doesn't exist.
     */
    public ProcessingElement getRemoteInstancesForKey() {
        logger.warn("The getRemoteInstancesForKey() method is not implemented. Use "
                + "it to test your app in single node configuration only. Should work "
                + "transparently for remote objects once it is implemented.");

        ProcessingElement pe = peInstances.asMap().get(id);
        return pe;
    }

    /**
     * This method returns an immutable map that contains all the PE instances for this prototype. PE instances may be
     * located anywhere in the cluster. Be aware that this could be an expensive operation. TODO: not implemented for
     * cluster configuration yet, use it only in single node configuration. for testing apps.
     */
    public Map<String, ProcessingElement> getRemoteInstances() {

        logger.warn("The getRemoteInstances() method is not implemented. Use "
                + "it to test your app in single node configuration only. Should work "
                + "transparently for remote objects once it is implemented.");

        /*
         * For now we just return a copy as a placeholder. We need to implement a custom map capable of working on an S4
         * cluster as efficiently as possible.
         */
        return ImmutableMap.copyOf(peInstances.asMap());
    }

    /**
     * Unique ID for a PE instance.
     * 
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * The {@code ProcessingElement} prototype for this object.
     * 
     * @return the corresponding {@code ProcessingElement} for this instance.
     */
    public ProcessingElement getPrototype() {
        return pePrototype;
    }

    /**
     * This method exists simply to make <code>clone()</code> protected.
     */
    @Override
    protected Object clone() {
        try {
            Object clone = super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper method to be used by PE implementation classes. Sends an event to all the target streams.
     * 
     */
    protected <T extends Event> void emit(T event, Stream<T>[] streamArray) {

        for (int i = 0; i < streamArray.length; i++) {
            streamArray[i].put(event);
        }
    }

    private class OnTimeTask extends TimerTask {

        @Override
        public void run() {

            for (Map.Entry<String, ProcessingElement> entry : getPEInstances().entrySet()) {

                ProcessingElement peInstance = entry.getValue();

                try {
                    if (isThreadSafe) {
                        peInstance.onTime();
                    } else {
                        synchronized (peInstance) {
                            peInstance.onTime();
                        }
                    }
                } catch (Exception e) {
                    logger.error("Caught exception in timer when calling PE instance [{}] with id [{}].", peInstance,
                            peInstance.id);
                    logger.error("Timer error.", e);
                }
            }
        }
    }

    /**
     * @return the PE name
     */
    protected String getName() {
        return name;
    }

    /**
     * @param name
     *            PE name
     */
    protected void setName(String name) {

        if (name == null)
            return;

        this.name = name;
        if (app.peByName.containsKey(name)) {
            logger.warn("Using a duplicate PE name: [{}]. This is probbaly not what you wanted.", name);
        }
        app.peByName.put(name, this);
    }

    public CheckpointingConfig getCheckpointingConfig() {
        return checkpointingConfig;
    }

    public void setCheckpointingConfig(CheckpointingConfig checkpointingConfig) {
        this.checkpointingConfig = checkpointingConfig;
    }

    /**
     * By default, the state of a PE instance is considered dirty whenever it processed an event. Some event may
     * actually leave the state of the PE unchanged. PE implementations can therefore override this method to
     * accommodate specific behaviors, by managing a custom "dirty" flag.
     * 
     * <b>If this method is overriden, {@link #clearDirty()} method must also be overriden in order to correctly reflect
     * the "dirty" state of the PE.</b>
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Dirty state is cleared after the PE has been serialized. PE implementations that maintain their "dirty" flag must
     * override this method by clearing their internally managed "dirty" flag.
     * 
     * <b>If this method is overriden, {@link #isDirty()} must also be overriden in order to correctly reflect the
     * "dirty" state of the PE.</b>
     */
    public void clearDirty() {
        this.dirty = false;
    }

    public byte[] serializeState() {
        return getApp().getSerDeser().serialize(this).array();
    }

    public ProcessingElement deserializeState(byte[] loadedState) {
        return (ProcessingElement) getApp().getSerDeser().deserialize(ByteBuffer.wrap(loadedState));
    }

    public void restoreState(ProcessingElement oldState) {
        restoreFieldsForClass(oldState.getClass(), oldState);
    }

    protected void recover() {
        byte[] serializedState = null;
        try {
            serializedState = getApp().getCheckpointingFramework().fetchSerializedState(new CheckpointId(this));
        } catch (RuntimeException e) {
            logger.error("Cannot fetch serialized stated for [{}/{}]: {}", new String[] {
                    getPrototype().getClass().getName(), getId(), e.getMessage() });
        }
        if (serializedState == null) {
            return;
        }
        try {
            ProcessingElement peInOldState = deserializeState(serializedState);
            restoreState(peInOldState);
        } catch (RuntimeException e) {
            logger.error("Cannot restore state for key [" + new CheckpointId(this) + "]: " + e.getMessage(), e);
        }
    }

    private void restoreFieldsForClass(Class<?> currentInOldStateClassHierarchy, ProcessingElement oldState) {
        if (!ProcessingElement.class.isAssignableFrom(currentInOldStateClassHierarchy)) {
            return;
        } else {
            Field[] fields = oldState.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isTransient(field.getModifiers()) && !Modifier.isStatic(field.getModifiers())) {
                    if (!Modifier.isPublic(field.getModifiers())) {
                        field.setAccessible(true);
                    }
                    try {
                        // TODO use reflectasm
                        field.set(this, field.get(oldState));
                    } catch (IllegalArgumentException e) {
                        logger.error("Cannot recover old state for this PE [{}]", e);
                        return;
                    } catch (IllegalAccessException e) {
                        logger.error("Cannot recover old state for this PE [{}]", e);
                        return;
                    }

                }
            }
            restoreFieldsForClass(currentInOldStateClassHierarchy.getSuperclass(), oldState);
        }
    }

    class Trigger {
        final long intervalInMilliseconds;
        final int intervalInEvents;
        long lastTime;
        int eventCount;
        // inactive triggers never trigger anything, they are used as markers
        boolean active = true;

        Trigger() {
            this.intervalInEvents = 0;
            this.intervalInMilliseconds = 0;
            this.active = false;
        }

        Trigger(int intervalInEvents, long intervalInMilliseconds) {
            this.intervalInEvents = intervalInEvents;
            this.intervalInMilliseconds = intervalInMilliseconds;
        }

        boolean checkAndUpdate() {
            if (active) {
                long timeLapse = System.currentTimeMillis() - lastTime;
                eventCount++;
                lastTime = System.currentTimeMillis();

                if (timeLapse > intervalInMilliseconds || eventCount >= intervalInEvents) {
                    eventCount = 0;
                    return true;
                }
            }
            return false;
        }

        boolean isActive() {
            return active;
        }
    }

    public long getEventCount() {
        return eventCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getName() + "/" + getId() + " ;");
        if (isSingleton) {
            sb.append("singleton ;");
        }
        sb.append(isThreadSafe ? "IS thread-safe ;" : "Not thread-safe ;");
        sb.append("timerInterval=" + timerIntervalInMilliseconds + " ;");
        return sb.toString();

    }

}
