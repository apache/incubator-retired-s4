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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TopNTopicPE extends ProcessingElement {

    static Logger logger = LoggerFactory.getLogger(TopNTopicPE.class);
    Map<String, Integer> countedTopics = Maps.newHashMap();

    public TopNTopicPE() {
        // required for checkpointing. Requirement to be lifted in 0.6
    }

    public TopNTopicPE(App app) {
        super(app);
        logger.info("key: [{}]", getId());
    }

    public void onEvent(TopicEvent event) {
        countedTopics.put(event.getTopic(), event.getCount());
    }

    public void onTime() {
        TreeSet<TopNEntry> sortedTopics = Sets.newTreeSet();
        for (Map.Entry<String, Integer> topicCount : countedTopics.entrySet()) {
            sortedTopics.add(new TopNEntry(topicCount.getKey(), topicCount.getValue()));
        }

        File f = new File("TopNTopics.txt");

        StringBuilder sb = new StringBuilder();
        int i = 0;
        Iterator<TopNEntry> iterator = sortedTopics.iterator();
        sb.append("----\n" + new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss").format(new Date()) + "\n");

        while (iterator.hasNext() && i < 10) {
            TopNEntry entry = iterator.next();
            sb.append("topic [" + entry.topic + "] count [" + entry.count + "]\n");
            i++;
        }
        sb.append("\n");
        try {
            Files.append(sb.toString(), f, Charsets.UTF_8);
            logger.info("Wrote top 10 topics to file [{}] ", f.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Cannot write top 10 topics to file [{}]", f.getAbsolutePath(), e);
        }
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    class TopNEntry implements Comparable<TopNEntry> {
        String topic = null;
        int count = 0;

        public TopNEntry(String topic, int count) {
            this.topic = topic;
            this.count = count;
        }

        public int compareTo(TopNEntry topNEntry) {
            if (topNEntry.count < this.count) {
                return -1;
            } else if (topNEntry.count > this.count) {
                return 1;
            }
            return 0;
        }

        public String toString() {
            return "topic:" + topic + " count:" + count;
        }
    }
}
