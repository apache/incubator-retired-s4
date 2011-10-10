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


/*
 * This is provided for cases where we want to process all events in 
 * a given node without a key. If the application class extends this class, 
 * then you will not be able to use it with keys limiting reusability.
 */
public abstract class SingletonPE extends ProcessingElement {

    public SingletonPE(App app) {
        super(app);
    }

    /* Return the only PE instance . */
    public ProcessingElement getInstanceForKey() {

        return this;
    }

    /* Ignore key, there is only one instance. */
    @Override
    public ProcessingElement getInstanceForKey(String id) {

        return this;
    }

//    abstract protected void onEvent(Event event);
//
//    abstract public void onTrigger(Event event);

    /*
     * Don't let subclasses override this method. It is not needed. All
     * initialization should be done by the concrete PE constructor.
     */
    @Override
    final protected void onCreate() {
        // TODO Auto-generated method stub

    }
}
