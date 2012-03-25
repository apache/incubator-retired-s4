package org.apache.s4.example.twitter;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

// keyed by topic name
public class TopicCountAndReportPE extends ProcessingElement {

    Stream<TopicSeenEvent> downStream;
    int threshold = 10;
    int count;

    public TopicCountAndReportPE(App app) {
        super(app);
        // TODO Auto-generated constructor stub
    }

    public void setDownstream(Stream<TopicSeenEvent> stream) {
        this.downStream = stream;
    }

    public void onEvent(TopicSeenEvent event) {
        count += event.count;
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void onTime() {
        if (count < threshold) {
            return;
        }
        TopicSeenEvent topicSeenEvent = new TopicSeenEvent(getId(), count);
        downStream.put(topicSeenEvent);
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
