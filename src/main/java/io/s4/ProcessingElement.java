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
package io.s4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProcessingElement implements Cloneable {

    Logger logger = LoggerFactory.getLogger(ProcessingElement.class);

    final protected App app;
    final protected Map<String, ProcessingElement> peInstances = new ConcurrentHashMap<String, ProcessingElement>();
    protected String id = ""; // PE instance id
    final protected ProcessingElement pePrototype;

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

    synchronized public void handleInputEvent(Event event) {

        processInputEvent(event);
    }

    abstract protected void processInputEvent(Event event);

    abstract public void sendEvent(); // consider having several output
                                      // policies...

    abstract protected void initPEInstance();

    abstract protected void removeInstanceForKey(String id);

    private void removeInstanceForKeyInternal(String id) {

        if (id == null)
            return;

        /* First let the PE instance clean after itself. */
        removeInstanceForKey(id);

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

    synchronized public ProcessingElement getInstanceForKey(String id) {

        /* Check if instance for key exists, otherwise create one. */
        ProcessingElement pe = peInstances.get(id);
        if (pe == null) {
            /* PE instance for key does not yet exist, cloning one. */
            pe = (ProcessingElement) this.clone();
            peInstances.put(id, pe);
            pe.id = id;
            pe.initPEInstance();

            logger.trace("Num PE instances: {}.", getNumPEInstances());
        }
        return pe;
    }

    synchronized protected List<ProcessingElement> getAllInstances() {

        return new ArrayList<ProcessingElement>(peInstances.values());
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
