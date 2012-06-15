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
