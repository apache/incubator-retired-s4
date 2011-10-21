/*
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.core;

import java.util.Collection;
import java.util.Map;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.apache.s4.base.Event;
import org.apache.s4.core.gen.OverloadDispatcher;
import org.apache.s4.core.gen.OverloadDispatcherGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableMap;

/**
 * @author Leo Neumeyer
 * @author Matthieu Morel
 *         <p>
 *         Base class for implementing processing in S4. All instances are
 *         organized as follows:
 *         <ul>
 *         <li>A PE prototype is a special type of instance that, along with
 *         {@link Stream} defines the topology of the application graph.
 *         <li>PE prototypes manage the creation and destruction of PE
 *         instances.
 *         <li>All PE instances are clones of a PE prototype.
 *         <li>PE instances are associated with a unique key.
 *         <li>PE instances do the actual work by processing any number of input
 *         events of various types and emit output events of various types.
 *         <li>To process events, {@code ProcessingElement} dynamically matches
 *         an event type to a processing method. See
 *         {@link org.apache.s4.core.gen.OverloadDispatcher} . There are two
 *         types of processing methods:
 *         <ul>
 *         <li>{@code onEvent(SomeEvent event)} When implemented, input events
 *         of type {@code SomeEvent} will be dispatched to this method.
 *         <li>{@code onTrigger(AnotherEvent event)} When implemented, input
 *         events of type {@code AnotherEvent} will be dispatched to this method
 *         when certain conditions are met. See
 *         {@link #setTrigger(Class, int, long, TimeUnit)}.
 *         </ul>
 *         <li>
 *         A PE implementation must not create threads. A periodic task can be
 *         implemented by overloading the {@link #onTime()} method. See
 *         {@link #setTimerInterval(long, TimeUnit)}
 *         <li>If a reference in the PE prototype shared by the PE instances,
 *         the object must be thread safe.
 *         <li>The code in a PE instance is synchronized by the framework to
 *         avoid concurrency problems.
 *         <li>In some special cases, it may be desirable to allow concurrency
 *         in the PE instance. For example, there may be several event
 *         processing methods that can safely run concurrently. To enable
 *         concurrency, annotate the implementation of {@code ProcessingElement}
 *         with {@link ThreadSafe}.
 *         <li>PE instances never use the constructor. They must be initialized
 *         by implementing the {@link #onCreate()} method.
 *         <li>PE class fields are cloned from the prototype. References are
 *         also copied which means that if the prototype creates a collection
 *         object, all instances will be sharing the same collection object
 *         which is usually <em>NOT</em> what the programmer intended . The
 *         application developer is responsible for initializing objects in the
 *         {@link #onCreate()} method. For example, if each instance requires a
 *         <tt>List<tt/> object the PE should implement the following:
 *         <pre>
 *         {@code
 *         public class MyPE extends ProcessingElement {
 * 
 *           private Map<String, Integer> wordCount;
 *         
 *           ...
 *         
 *           onCreate() {
 *           wordCount = new HashMap<String, Integer>;
 *           logger.trace("Created a map for instance PE with id {}, getId());
 *           }
 *         }
 *         }
 *         </pre>
 * 
 * 
 *         </ul>
 * 
 * 
 * 
 * 
 */
public abstract class ProcessingElement implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingElement.class);

    protected App app;

    /*
     * This maps holds all the instances. We make it package private to prevent
     * concrete classes from updating the collection.
     */
    Cache<String, ProcessingElement> peInstances;

    /* This map is initialized in the prototype and cloned to instances. */
    Map<Class<? extends Event>, Trigger> triggers;

    /* PE instance id. */
    String id = "";

    /* Private fields. */
    private ProcessingElement pePrototype;
    private boolean haveTriggers = false;
    private long timerIntervalInMilliseconds = 0;
    private Timer timer;
    private boolean isPrototype = true;
    private boolean isThreadSafe = false;
    private boolean isFirst = true;

    private transient OverloadDispatcher overloadDispatcher;

    protected ProcessingElement() {
    }

    /**
     * Create a PE prototype. By default, PE instances will never expire. Use
     * {@code #configurePECache} to configure.
     * 
     * @param app
     *            the app that contains this PE
     */
    public ProcessingElement(App app) {
        OverloadDispatcherGenerator oldg = new OverloadDispatcherGenerator(this.getClass());
        Class<?> overloadDispatcherClass = oldg.generate();
        try {
            overloadDispatcher = (OverloadDispatcher) overloadDispatcherClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.app = app;
        app.addPEPrototype(this);

        peInstances = CacheBuilder.newBuilder().build(new CacheLoader<String, ProcessingElement>() {
            @Override
            public ProcessingElement load(String key) throws Exception {
                return createPE(key);
            }
        });

        triggers = new MapMaker().makeMap();

        /*
         * Only the PE Prototype uses the constructor. The PEPrototype field
         * will be cloned by the instances and point to the prototype.
         */
        this.pePrototype = this;
    }

    /**
     * This method is called by the PE timer. By default it is synchronized with
     * the {@link #onEvent()} and {@link #onTrigger()} methods. To execute
     * concurrently with other methods, the {@link ProcessingElelment} subclass
     * must be annotated with {@link @ThreadSafe}.
     * 
     * Override this method to implement a periodic process.
     */
    void onTime() {
    }

    /**
     * This method is called after a PE instance is created. Use it to
     * initialize fields that are PE instance specific. PE instances are created
     * using {#clone()}. Fields initialized in the class constructor are shared
     * by all PE instances.
     */
    abstract protected void onCreate();

    /**
     * This method is called before a PE instance is removed. Use it to close
     * resources and clean up.
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

    /**
     * Returns the approximate number of PE instances from the cache.
     * 
     * @return the approximate number of PE instances.
     */
    public long getNumPEInstances() {

        return peInstances.size();
    }

    /**
     * Set PE expiration and cache size.
     * <p>
     * PE instances will be automatically removed from the cache once a fixed
     * duration has elapsed after the PEs creation, or last access.
     * <p>
     * Least accessed PEs will automatically be removed from the cache when the
     * number of PEs approaches maximumSize.
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
     */
    public void configurePECache(int maximumSize, long duration, TimeUnit timeUnit) {

        if (!isPrototype)
            return;

        peInstances = CacheBuilder.newBuilder().expireAfterAccess(duration, timeUnit).maximumSize(maximumSize)
                .build(new CacheLoader<String, ProcessingElement>() {
                    @Override
                    public ProcessingElement load(String key) throws Exception {
                        return createPE(key);
                    }
                });
    }

    /**
     * This trigger is fired when the following conditions occur:
     * 
     * <ul>
     * <li>An event of eventType arrived to the PE instance
     * <li>numEvents have arrived since the last time this trigger was fired
     * -OR- time since last event is greater than interval.
     * </ul>
     * 
     * <p>
     * When the trigger fires, the method <tt>trigger(EventType event)</tt> is
     * called. Where <tt>EventType</tt> matches the argument eventType.
     * 
     * @param eventType
     *            the type of event on which this trigger will fire.
     * @param numEvents
     *            number of events since last trigger activation. Must be
     *            greater than zero. (Set to one to trigger on every input
     *            event.)
     * @param interval
     *            minimum time between triggers. Set to zero if no time interval
     *            needed.
     * @param timeUnit
     *            the TimeUnit for the argument interval. Can set to null if no
     *            time interval needed.
     */
    public void setTrigger(Class<? extends Event> eventType, int numEvents, long interval, TimeUnit timeUnit) {

        if (!isPrototype) {
            logger.warn("This method can only be used on the PE prototype.");
            return;
        }

        if (eventType == null) {
            logger.error("Argument null in setTrigger() method is not valid.");
            return;
        }

        if (numEvents < 1) {
            logger.error("Argument numEvents in setTrigger() method must be greater than zero.");
            return;
        }

        /* Skip trigger checking overhead if there are no triggers. */
        haveTriggers = true;

        long intervalInMilliseconds = 0;
        if (timeUnit != null)
            intervalInMilliseconds = timeUnit.convert(interval,
                    TimeUnit.MILLISECONDS);

        Trigger config = new Trigger(numEvents, intervalInMilliseconds);

        triggers.put(eventType, config);
    }

    /**
     * The duration of the periodic task controlled by the embedded timer.
     * 
     * @param timeUnit
     *            the timeUnt of the returned value.
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
    public void setTimerInterval(long interval, TimeUnit timeUnit) {
        timerIntervalInMilliseconds = TimeUnit.MILLISECONDS.convert(interval, timeUnit);

        /* We only allow timers in the PE prototype, not in the instances. */
        if (!isPrototype) {
            logger.warn("This method can only be used on the PE prototype.");
            return;
        }

        if (timer != null || interval == 0)
            timer.cancel();

        timer = new Timer();
        timer.schedule(new OnTimeTask(), 0, timerIntervalInMilliseconds);
    }

    /**
     * Set to true if the concrete PE class has the {@link ThreadSafe}
     * annotation. The default is false (no annotation). In general, application
     * developers don't need to worry about thread safety in the concrete PEs.
     * In some cases the PE needs to be thread safe to avoid deadlocks. For
     * example , if the application graph has cycles and the queues are allowed
     * to block, then some critical PEs with multiple incoming streams need to
     * be made thread safe to avoid locking the entire PE instance.
     * 
     * @return true if the PE implementation is considered thread safe.
     */
    public boolean isThreadSafe() {
        return isThreadSafe;
    }

    protected void handleInputEvent(Event event) {

        Object object;
        if (isThreadSafe) {
            object = new Object(); // a dummy object TODO improve this.
        } else {
            object = this;
        }

        synchronized (object) {

            /* Dispatch onEvent() method. */
            overloadDispatcher.dispatchEvent(this, event);

            /* Dispatch onTrigger() method. */
            if (haveTriggers && isTrigger(event)) {
                overloadDispatcher.dispatchTrigger(this, event);
            }
        }
    }

    private boolean isTrigger(Event event) {
        return isTrigger(event, event.getClass());
    }

    /**
     * Checks the trigger for this event type. 
     * Creates an inactive trigger if no trigger is found after recursively exploring 
     * the event class hierarchy.
     * An inactive trigger never triggers.
     * 
     * @return true if trigger is reached, 
     *         false if trigger is not ready yet or if trigger is inactive
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
        if (timer != null) {
            timer.cancel();
            logger.info("Timer stopped.");
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
        if (isFirst)
            onCreateInternal(pe);
        pe.id = id;
        pe.onCreate();
        logger.trace("Num PE instances: {}.", getNumPEInstances());
        return pe;
    }

    /**
     * This method is designed to be used within the package. We make it
     * package-private. The returned instances are all in the same JVM. Do not
     * use it to access remote objects.
     */
    public ProcessingElement getInstanceForKey(String id) {

        /* Check if instance for key exists, otherwise create one. */
        try {
            return peInstances.get(id);
        } catch (ExecutionException e) {
            logger.error("Problem when trying to create a PE instance.", e);
        }

        return null;
    }

    /**
     * Get all the local instances. See notes in
     * {@link #getInstanceForKey(String) getLocalInstanceForKey}
     */
    public Collection<ProcessingElement> getInstances() {

        return peInstances.asMap().values();
    }

    /**
     * This method returns a remote PE instance for key. TODO: not implemented
     * for cluster configuration yet, use it only in single node configuration.
     * for testing apps.
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
     * This method returns an immutable map that contains all the PE instances
     * for this prototype. PE instances may be located anywhere in the cluster.
     * Be aware that this could be an expensive operation. TODO: not implemented
     * for cluster configuration yet, use it only in single node configuration.
     * for testing apps.
     */
    public Map<String, ProcessingElement> getRemoteInstances() {

        logger.warn("The getRemoteInstances() method is not implemented. Use "
                + "it to test your app in single node configuration only. Should work "
                + "transparently for remote objects once it is implemented.");

        /*
         * For now we just return a copy as a placeholder. We need to implement
         * a custom map capable of working on an S4 cluster as efficiently as
         * possible.
         */
        return ImmutableMap.copyOf(peInstances.asMap());
    }

    /*
     * Called when we create the first PE instance. TODO: Would be better to do
     * this as part of the PE lifecycle after PE construction.
     */
    private void onCreateInternal(ProcessingElement pe) {

        isFirst = false;

        logger.trace("OnCreateInternal");

        /*
         * If PE class has the @ThreadSafe annotation, then we set isThtreadSafe
         * to true in the prototype so all future PE instances inherit the
         * setting.
         */
        if (pe.getClass().isAnnotationPresent(ThreadSafe.class) == true) {
            pe.isThreadSafe = true;
            isThreadSafe = true;

            logger.trace("Annotated with @ThreadSafe");
        }

        /*
         * Each PE instance needs its own triggers map to keep track time lapsed
         * and event count.
         */
        pe.triggers = Maps.newHashMap(triggers);
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

    private class OnTimeTask extends TimerTask {

        @Override
        public void run() {

            for (Map.Entry<String, ProcessingElement> entry : peInstances.asMap().entrySet()) {

                ProcessingElement peInstance = entry.getValue();

                if (isThreadSafe) {
                    peInstance.onTime();
                } else {
                    synchronized (this) {
                        peInstance.onTime();
                    }
                }
            }
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

}
