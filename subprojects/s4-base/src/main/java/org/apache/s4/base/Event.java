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
package org.apache.s4.base;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class Event {

    private static final Logger logger = LoggerFactory.getLogger(Event.class);

    final private long time;
    private int streamId;
    private int appId;
    private Map<String, Data<?>> map;

    /** Default constructor sets time using system time. */
    protected Event() {
        this.time = System.currentTimeMillis();
    }

    /**
     * This constructor explicitly sets the time. Event that need to explicitly set the time must call {super(time)}
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
     * The stream id is used to identify streams uniquely in a cluster configuration. It is not required to operate in
     * local mode.
     * 
     * @return the target stream id
     */
    public int getStreamId() {
        return streamId;
    }

    /**
     * The stream id is used to identify streams uniquely in a cluster configuration. It is not required to operate in
     * local mode.
     * 
     * @param targetStreamId
     */
    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    /**
     * All events must be assigned the unique App ID of the App that owns the stream to which this event is injected.
     * The assignment must be done automatically by the stream that receives the event. Each application has a unique
     * ID. We use the app ID in combination with the stream ID to identify stream instances in a cluster.
     * 
     * 
     * @return the unique application ID.
     */
    public int getAppId() {
        return appId;
    }

    /**
     * All events must be assigned the unique App ID of the App that owns the stream to which this event is injected.
     * The assignment must be done automatically by the stream that receives the event. Each application has a unique
     * ID. We use the app ID in combination with the stream ID to identify stream instances in a cluster.
     * 
     * @param appId
     *            a unique application identifier, typically assigned by the deployment system.
     */
    public void setAppId(int appId) {
        this.appId = appId;
    }

    /**
     * Put an arbitrary key-value pair in the event. The type of the value is T.
     * 
     * @param type
     *            the type of the value
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public <T> void put(String key, Class<T> type, T value) {

        if (map == null) {
            map = Maps.newHashMap();
        }

        map.put(key, new Data<T>(type, value));
    }

    /**
     * Get value for key. The caller must know the type of the value.
     * 
     * @param key
     * @return the value
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {

        Data<?> data = map.get(key);

        if (type != data.type) {
            logger.error("Trying to get a value of type {} for an attribute of type {}.", type, data.type);
            return null;
        }

        return (T) data.value;
    }

    /* Helper data object. */
    private class Data<T> {

        private T value;
        private Class<T> type;

        private Data(Class<T> type, T value) {
            this.value = value;
            this.type = type;
        }

    }
}
