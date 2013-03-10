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

package org.apache.s4.core.adapter;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.RemoteStream;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Base class for adapters. For now, it provides facilities for automatically creating an output stream.
 * <p>
 * This class can be used for easing the injection of events into S4 applications.
 * 
 */
public abstract class AdapterApp extends App {

    @Inject
    @Named(value = "s4.adapter.output.stream")
    String outputStreamName;

    private RemoteStream remoteStream;

    protected KeyFinder<Event> remoteStreamKeyFinder;

    public RemoteStream getRemoteStream() {
        return remoteStream;
    }

    @Override
    protected void onStart() {
    }

    @Override
    protected void onInit() {
        remoteStream = createOutputStream(outputStreamName, remoteStreamKeyFinder);
    }

    /**
     * This method allows to specify a keyfinder in order to partition the output stream
     * 
     * @param keyFinder
     *            used for identifying keys from the events
     */
    protected void setKeyFinder(KeyFinder<Event> keyFinder) {
        this.remoteStreamKeyFinder = keyFinder;
    }

    @Override
    protected void onClose() {
    }

}
