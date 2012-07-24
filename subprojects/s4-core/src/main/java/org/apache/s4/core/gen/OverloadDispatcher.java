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

package org.apache.s4.core.gen;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;


/**
 * This interface defines methods for dispatching input and output events to the
 * most relevant methods of the processing elements.
 * 
 * <p>
 * <code>onEvent</code> and <code>onTrigger</code> methods
 * may be overloaded with different subtypes of {@link Event} parameters.
 * Methods defined in this interface dispatch an event to the method
 * which is the best match according to the runtime type of the event.
 * </p>
 * <p>Example: consider PE of type <code>ExamplePE extends ProcessingElement</code> that defines methods:
 * <ul>
 * <li><code>onEvent(EventA event)</code></li> 
 * <li><code>onEvent(EventB event)</code></li>
 * </ul>
 * </p>
 * <p>Then:
 * <ul>
 * <li>When event1 of type <code>EventA extends Event</code> is received on this PE, it will be handled by method <code>onEvent(EventA event)</code></li>
 * <li>When event2 of type <code>EventB extends EventA</code> is received on this PE, it will be handled by method <code>onEvent(EventB event)</code></li>
 * </ul>
 * </p>
 * <p>
 * Implementations of this interface are typically generated at runtime.
 * </p>
 */
public interface OverloadDispatcher {

    public void dispatchEvent(ProcessingElement pe, Event event);

    public void dispatchTrigger(ProcessingElement pe, Event event);
}
