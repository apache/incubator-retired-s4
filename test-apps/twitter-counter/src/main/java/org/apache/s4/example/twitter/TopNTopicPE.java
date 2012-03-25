package org.apache.s4.example.twitter;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TopNTopicPE extends ProcessingElement {

    public TopNTopicPE(App app) {
        super(app);
        // TODO Auto-generated constructor stub
    }

    Map<String, Integer> countedTopics = Maps.newHashMap();
    static Logger logger = LoggerFactory.getLogger(TopNTopicPE.class);

    public void onEvent(TopicSeenEvent event) {
        countedTopics.put(event.topic, event.count);
    }

    public void onTime() {
        TreeSet<TopNEntry> sortedTopics = Sets.newTreeSet();
        for (Map.Entry<String, Integer> topicCount : countedTopics.entrySet()) {
            sortedTopics.add(new TopNEntry(topicCount.getKey(), topicCount.getValue()));
        }

        int i = 0;
        Iterator<TopNEntry> iterator = sortedTopics.iterator();
        long time = System.currentTimeMillis();
        while (iterator.hasNext() && i < 10) {
            TopNEntry entry = iterator.next();
            logger.info("{} : topic [{}] count [{}]",
                    new String[] { String.valueOf(time), entry.topic, String.valueOf(entry.count) });
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
