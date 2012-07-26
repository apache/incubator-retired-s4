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

package s4app;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ProducerPE.class);

    private Streamable[] targetStreams;

    public ProducerPE(App app) {
        super(app);
    }

    /**
     * @param targetStreams
     *            the {@link Streamable} streams.
     */
    public void setStreams(Streamable... targetStreams) {
        this.targetStreams = targetStreams;
    }

    public void sendMessages() {
        for (long tick = 1; tick <= 100000; tick++) {
            Event event = new Event();
            event.put("tick", Long.class, tick);

            logger.trace("Sending event with tick {} and time {}.", event.get("tick", Long.class), event.getTime());
            for (int i = 0; i < targetStreams.length; i++) {
                targetStreams[i].put(event);
            }
        }
    }

    @Override
    protected void onRemove() {

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }
}
