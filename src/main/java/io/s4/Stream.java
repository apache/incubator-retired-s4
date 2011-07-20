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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Stream<T extends Event> implements Runnable {

    final static private String DEFAULT_SEPARATOR = "^";
    final static private int CAPACITY = 1000;
    final private String name;
    final private Key<T> key;
    final private ProcessingElement[] targetPEs;
    final private BlockingQueue<T> queue = new ArrayBlockingQueue<T>(CAPACITY);

    public Stream(String name, KeyFinder<T> finder,
            ProcessingElement... processingElements) {
        this.name = name;
        this.key = new Key<T>(finder, DEFAULT_SEPARATOR);
        this.targetPEs = processingElements;

        /* Start streaming. */
        /*
         * TODO: This is only for prototyping. Comm layer will take care of this
         * in the real implementation.
         */
        new Thread(this).start();
    }

    public void put(T event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the key
     */
    public Key<T> getKey() {
        return key;
    }

    /**
     * @return the list of target processing elements.
     */
    public ProcessingElement[] getTargetPEs() {
        return targetPEs;
    }

    @Override
    public void run() {
        while(true) {
            try {
                /* Get oldest event in queue. */
                T event = queue.take();
                
                /* Send event to each target PE. */
                for(int i=0; i<targetPEs.length; i++) {
                    
                    /* STEP 1: find the PE instance for key. */
                    ProcessingElement pe = targetPEs[i].getInstanceForKey(key.get(event));
                    
                    /* STEP 2: pass event to PE instance. */
                    pe.handleInputEvent(event);
                }
                
                
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
