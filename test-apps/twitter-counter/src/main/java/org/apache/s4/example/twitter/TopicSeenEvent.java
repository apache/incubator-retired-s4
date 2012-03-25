package org.apache.s4.example.twitter;

import org.apache.s4.base.Event;

public class TopicSeenEvent extends Event {

    public String topic;
    public int count;
    public String reportKey = "x";

    public TopicSeenEvent(String topic, int count) {
        super();
        this.topic = topic;
        this.count = count;
    }

}
