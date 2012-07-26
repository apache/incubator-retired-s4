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

import java.net.ServerSocket;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class TopicExtractorPE extends ProcessingElement {

    static private ServerSocket serverSocket;
    Streamable<Event> downStream;
    static Logger logger = LoggerFactory.getLogger(TopicExtractorPE.class);

    public TopicExtractorPE(App app) {
        super(app);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void onCreate() {

    }

    public void setDownStream(Streamable<Event> stream) {
        this.downStream = stream;
    }

    public void onEvent(Event event) {
        String text = event.get("statusText", String.class);
        logger.trace("event text [{}]", text);
        if (!text.contains("#")) {
            return;
        }
        Iterable<String> split = Splitter.on(' ').omitEmptyStrings().trimResults().split(text);
        for (String topic : split) {
            if (!topic.startsWith("#")) {
                continue;
            }
            String topicOnly = topic.substring(1);
            if (topicOnly.length() == 0 || topicOnly.contains("#")) {
                continue;
            }
            downStream.put(new TopicEvent(topicOnly, 1));
        }
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
