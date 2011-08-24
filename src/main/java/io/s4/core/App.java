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

import java.util.ArrayList;
import java.util.List;

/*
 * Container base class to hold all processing elements. We will implement administrative methods here. 
 */
public abstract class App {
    
    final private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();
    final private List<Stream<? extends Event>> streams = new ArrayList<Stream<? extends Event>>();

    /**
     * @return the pePrototypes
     */
    public List<ProcessingElement> getPePrototypes() {
        return pePrototypes;
    }

    protected abstract void start();

    protected abstract void init();

    protected abstract void close();

    public void removeAll() {

        for (ProcessingElement pe : pePrototypes) {

            /* Remove all instances. */
            pe.removeAll();

        }

        for (Stream<? extends Event> stream : streams) {

            /* Close all streams. */
            stream.close();
        }

        /* Give prototype a chance to clean up after itself. */
        close();

        /* Finally remove from App. */
        pePrototypes.clear();
        streams.clear();
    }

    void addPEPrototype(ProcessingElement pePrototype) {

        pePrototypes.add(pePrototype);

    }

    void addStream(Stream<? extends Event> stream) {

        streams.add(stream);

    }

    public List<Stream<? extends Event>> getStreams() {
        return streams;
    }
}
