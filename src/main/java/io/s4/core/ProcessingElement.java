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
package io.s4.core;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProcessingElement implements Cloneable {

    private static final Logger logger = LoggerFactory
            .getLogger(ProcessingElement.class);

    final protected App app;
    final protected ConcurrentMap<String, ProcessingElement> peInstances = new ConcurrentHashMap<String, ProcessingElement>();
    protected String id = ""; // PE instance id
    final protected ProcessingElement pePrototype;
    private int outputIntervalInEvents = 1;
    private int eventCount = 0;

    /*
     * Base class for implementing processing in S4. All instances are organized
     * as follows. A PE prototype is a special type of instance that defines the
     * topology of the graph and manages the creation and destruction of the
     * actual instances that do the processing. PE instances are clones of the
     * prototype. PE instance variables should be initialized in the
     * initPEInstance() method. Be aware that Class variables are simply copied
     * to the clones, even references.
     */
    public ProcessingElement(App app) {

        this.app = app;
        app.addPEPrototype(this);

        /*
         * Only the PE Prototype uses the constructor. The PEPrototype field
         * will be cloned by the instances and point to the prototype.
         */
        this.pePrototype = this;
    }

    /**
     * @return the app
     */
    public App getApp() {
        return app;
    }

    public int getNumPEInstances() {

        return peInstances.size();
    }

    /**
     * @return the outputIntervalInEvents - the number of input events after
     *         which we call {#processOutputEvent()}. Set to zero to stop
     *         calling the output method.
     */
    public int getOutputIntervalInEvents() {
        return outputIntervalInEvents;
    }

    /**
     * @param outputIntervalInEvents
     *            - the number of input events after which we call
     *            {#processOutputEvent()}. Set to zero to stop calling the
     *            output method.
     */
    public void setOutputIntervalInEvents(int outputIntervalInEvents) {
        this.outputIntervalInEvents = outputIntervalInEvents;
    }

    synchronized protected void handleInputEvent(Event event) {
//protected void handleInputEvent(Event event) { // we get deadlock when
                                                   // synchronized
        eventCount++;

            //System.out.println("XXX: " +  id + "  |  " + eventCount);
            processInputEvent(event);

        if (isOutput()) {
           processOutputEvent(event);
        }
    }

    private boolean isOutput() {

        // TODO implement time-based policy using a timer.

        if (outputIntervalInEvents > 0
                && (eventCount % outputIntervalInEvents == 0)) {
            return true;
        }

        return false;
    }

    abstract protected void processInputEvent(Event event);

    abstract public void processOutputEvent(Event event); // consider having
                                                          // several output
    // policies...

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

    private void removeInstanceForKeyInternal(String id) {

        if (id == null)
            return;

        /* First let the PE instance clean after itself. */
        onRemove();

        /* Remove PE instance. */
        peInstances.remove(id);
    }

    protected void removeAll() {

        /* Remove all the instances. */
        for (Map.Entry<String, ProcessingElement> entry : peInstances
                .entrySet()) {

            String key = entry.getKey();

            if (key != null)
                removeInstanceForKeyInternal(key);
        }

        /*
         * TODO: This object (the PE prototype) may still be referenced by other
         * objects at this point. For example a stream object may still be
         * referencing PEs.
         */
    }

    protected void close() {
        removeInstanceForKeyInternal(id);
    }

    protected ProcessingElement getInstanceForKey(String id) {

        /* Check if instance for key exists, otherwise create one. */
        ProcessingElement pe = peInstances.get(id);
        if (pe == null) {
            /* PE instance for key does not yet exist, cloning one. */
            pe = (ProcessingElement) this.clone();

            /*
             * The thread safe method putIfAbsent will most likely return null.
             * However, in the rare event that a thread created the a PE with
             * the same key a microsecond before, then pe2 will return the
             * recently created object. In that case, we discard the second
             * clone. With this precaution, we avoid synchronizing the
             * {#getInstanceForKey} method and avoid deadlocks.
             */
            ProcessingElement pe2 = peInstances.putIfAbsent(id, pe);

            if (pe2 != null) {
                pe = pe2;
            }

            pe.id = id;
            pe.onCreate();

            logger.trace("Num PE instances: {}.", getNumPEInstances());
        }
        return pe;
    }

    protected Collection<ProcessingElement> getAllInstances() {

        // return new ArrayList<ProcessingElement>(peInstances.values());
        return peInstances.values();
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
     * @param id
     *            the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * This method exists simply to make <code>clone()</code> protected.
     */
    protected Object clone() {
        try {
            Object clone = super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: Change equals and hashCode in ProcessingElement and
    // Stream so we can use sets as collection and make sure there are no
    // duplicate prototypes.
    // Great article: http://www.artima.com/lejava/articles/equality.html

}
