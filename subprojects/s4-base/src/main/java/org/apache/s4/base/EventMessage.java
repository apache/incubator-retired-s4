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

package org.apache.s4.base;

/**
 * <p>
 * Encapsulates application-level events of type {@link Event}.
 * </p>
 * <p>
 * Indeed, events that are defined at the application level can only be handled by the classloader of the corresponding
 * application.
 * </p>
 * <p>
 * Includes routing information (application name, stream name), so that this message can be dispatched at the
 * communication level.
 * </p>
 * 
 */
public class EventMessage {

    private String appName;
    private String streamName;
    private byte[] serializedEvent;

    public EventMessage() {
    }

    /**
     * 
     * @param appName
     *            name of the application
     * @param streamName
     *            name of the stream
     * @param serializedEvent
     *            application-specific {@link Event} instance in serialized form
     */
    public EventMessage(String appName, String streamName, byte[] serializedEvent) {
        super();
        this.appName = appName;
        this.streamName = streamName;
        this.serializedEvent = serializedEvent;
    }

    public String getAppName() {
        return appName;
    }

    public String getStreamName() {
        return streamName;
    }

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }

}
