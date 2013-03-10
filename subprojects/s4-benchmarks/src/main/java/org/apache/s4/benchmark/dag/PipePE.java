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
package org.apache.s4.benchmark.dag;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipePE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(PipePE.class);

    private Stream<Event> downstream;

    public void onEvent(Event event) {

        String value = event.get("value", String.class);
        logger.trace("PipePE : {} -> {}", getId(), value);
        Event outputEvent = new Event();
        // if we reuse the same key, with the same key finder, this event goes to the current node
        outputEvent.put("key", int.class, event.get("key", int.class));
        outputEvent.put("value", String.class, value + "->" + getId());
        downstream.put(outputEvent);
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void setDownstream(Stream<Event> downstream) {
        this.downstream = downstream;
    }

}
