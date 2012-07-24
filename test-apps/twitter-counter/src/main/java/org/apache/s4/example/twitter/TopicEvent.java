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

package org.apache.s4.example.twitter;

import org.apache.s4.base.Event;

/**
 * Transports the topic name and count information
 * 
 */
public class TopicEvent extends Event {

    String topic;
    int count;

    public TopicEvent() {
    }

    public TopicEvent(String topic, int count) {
        this.topic = topic;
        this.count = count;
    }

    public String getTopic() {
        return topic;
    }

    public int getCount() {
        return count;
    }

}
