package org.apache.s4.example.twitter;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TopNTopicPE extends ProcessingElement {

    static Logger logger = LoggerFactory.getLogger(TopNTopicPE.class);
    Map<String, Integer> countedTopics = Maps.newHashMap();

    public TopNTopicPE(App app) {
        super(app);
        logger.info("key: [{}]", getId());
    }

    public void onEvent(Event event) {
        countedTopics.put(event.get("topic"), event.get("count", Integer.class));
    }

    public void onTime() {
        TreeSet<TopNEntry> sortedTopics = Sets.newTreeSet();
        for (Map.Entry<String, Integer> topicCount : countedTopics.entrySet()) {
            sortedTopics.add(new TopNEntry(topicCount.getKey(), topicCount.getValue()));
        }

        logger.info("\n------------------");

        int i = 0;
        Iterator<TopNEntry> iterator = sortedTopics.iterator();
        long time = System.currentTimeMillis();
        while (iterator.hasNext() && i < 10) {
            TopNEntry entry = iterator.next();
            logger.info("{} : topic [{}] count [{}]",
                    new String[] { String.valueOf(time), entry.topic, String.valueOf(entry.count) });
            i++;
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
