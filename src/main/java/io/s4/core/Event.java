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
package io.s4.core;

public abstract class Event {

    final private long time;
    private int targetStreamId;

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
     * The target stream id is used to identify streams uniquely in a cluster
     * configuration. It is not required to operate in local mode.
     * 
     * @return the target stream id
     */
    public int getTargetStreamId() {
        return targetStreamId;
    }

    /**
     * The target stream id is used to identify streams uniquely in a cluster
     * configuration. It is not required to operate in local mode.
     * 
     * @param targetStreamId
     */
    public void setTargetStreamId(int targetStreamId) {
        this.targetStreamId = targetStreamId;
    }
}
