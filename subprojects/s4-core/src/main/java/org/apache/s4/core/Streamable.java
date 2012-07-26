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

import org.apache.s4.base.Event;

/**
 * We use this interface to put events into objects.
 * 
 * @param <T>
 */
public interface Streamable<T extends Event> {

    /**
     * Starting the stream starts the associated dequeuing thread.
     */
    void start();

    /**
     * Put an event into the streams.
     * 
     * @param event
     */
    public void put(Event event);

    /**
     * Stop and close all the streams.
     */
    public void close();

    /**
     * @return the name of this streamable object.
     */
    public String getName();
}
