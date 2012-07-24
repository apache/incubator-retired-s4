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

package org.apache.s4.core.triggers;

import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.core.TriggerTest;
import org.apache.s4.wordcount.SentenceKeyFinder;

import com.google.inject.Inject;

public class TriggeredApp extends App {

    public Stream<Event> stream;

    @Inject
    public TriggeredApp() {
        super();
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {

        TriggerablePE prototype = createPE(TriggerablePE.class);
        stream = createStream("stream", new SentenceKeyFinder(), prototype);
        switch (TriggerTest.triggerType) {
            case COUNT_BASED:
                prototype.setTrigger(Event.class, 1, 0, TimeUnit.SECONDS);
                break;
            case TIME_BASED:
                prototype.setTrigger(Event.class, 1, 1, TimeUnit.MILLISECONDS);
            default:
                break;
        }

    }

    @Override
    protected void onClose() {
    }

}
