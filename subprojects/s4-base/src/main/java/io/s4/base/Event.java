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
package io.s4.base;

public abstract class Event {

    final private long time;
    private int streamId;
    private int appId;

    /** Default constructor sets time using system time. */
    protected Event() {
        this.time = System.currentTimeMillis();
    }

    /**
     * This constructor explicitly sets the time. Event that need to explicitly
     * set the time must call {super(time)}
     */
    protected Event(long time) {
        this.time = time;
    }

    /**
     * @return the create time
     */
    public long getTime() {
        return time;
    }

    /**
     * The stream id is used to identify streams uniquely in a cluster
     * configuration. It is not required to operate in local mode.
     * 
     * @return the target stream id
     */
    public int getStreamId() {
        return streamId;
    }

    /**
     * The stream id is used to identify streams uniquely in a cluster
     * configuration. It is not required to operate in local mode.
     * 
     * @param targetStreamId
     */
    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    /**
     * All events must be assigned the unique App ID of the App that owns the
     * stream to which this event is injected. The assignment must be done
     * automatically by the stream that receives the event. Each application
     * has a unique ID. We use the app ID in combination with the stream
     * ID to identify stream instances in a cluster.
     * 
     * 
     * @return the unique application ID.
     */
    public int getAppId() {
        return appId;
    }

    /**
     * All events must be assigned the unique App ID of the App that owns the
     * stream to which this event is injected. The assignment must be done
     * automatically by the stream that receives the event. Each application
     * has a unique ID. We use the app ID in combination with the stream
     * ID to identify stream instances in a cluster.
     * 
     * @param appId
     *            a unique application identifier, typically assigned by the
     *            deployment system.
     */
    public void setAppId(int appId) {
        this.appId = appId;
    }
}
