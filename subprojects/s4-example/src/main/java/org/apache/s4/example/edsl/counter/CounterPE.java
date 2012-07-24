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

package org.apache.s4.example.edsl.counter;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(CounterPE.class);

    private Stream<CountEvent>[] countStream = null;

    public CounterPE(App app) {
        super(app);
    }

    /**
     * @return the countStream
     */
    public Stream<CountEvent>[] getCountStream() {
        return countStream;
    }

    /**
     * @param countStream
     *            the countStream to set
     */
    public void setCountStream(Stream<CountEvent>[] countStream) {
        this.countStream = countStream;
    }

    private long counter = 0;

    public void onEvent(Event event) {

        counter += 1;
        logger.trace("PE with id [{}] incremented counter to [{}].", getId(), counter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.ProcessingElement#sendOutputEvent()
     */
    public void onTrigger(Event event) {

        logger.trace("Sending count event for PE id [{}] with count [{}].", getId(), counter);
        CountEvent countEvent = new CountEvent(getId(), counter);
        emit(countEvent, countStream);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.ProcessingElement#init()
     */
    @Override
    public void onCreate() {

    }

    @Override
    public void onRemove() {

    }
}
