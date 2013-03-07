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
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Key;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.comm.topology.RemoteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream that dispatches events to interested apps in remote clusters
 * 
 */
public class RemoteStream implements Streamable<Event> {

    final private String name;
    final protected Key<Event> key;
    final static private String DEFAULT_SEPARATOR = "^";

    RemoteSenders remoteSenders;

    Hasher hasher;

    int id;
    final private App app;
    private static Logger logger = LoggerFactory.getLogger(RemoteStream.class);

    public RemoteStream(App app, String name, KeyFinder<Event> finder, RemoteSenders remoteSenders, Hasher hasher,
            RemoteStreams remoteStreams, String clusterName) {
        this.app = app;
        this.name = name;
        this.remoteSenders = remoteSenders;
        this.hasher = hasher;
        if (finder == null) {
            this.key = null;
        } else {
            this.key = new Key<Event>(finder, DEFAULT_SEPARATOR);
        }
        remoteStreams.addOutputStream(clusterName, name);

    }

    @Override
    public void put(Event event) {
        event.setStreamId(getName());

        if (key != null) {
            remoteSenders.send(key.get(event), event);
        } else {
            remoteSenders.send(null, event);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
