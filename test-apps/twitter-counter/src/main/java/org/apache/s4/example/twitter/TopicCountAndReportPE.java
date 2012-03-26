package org.apache.s4.example.twitter;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// keyed by topic name
public class TopicCountAndReportPE extends ProcessingElement {

    Stream<Event> downStream;
    int threshold = 10;
    int count;
    boolean firstEvent = true;

    static Logger logger = LoggerFactory.getLogger(TopicCountAndReportPE.class);

    public TopicCountAndReportPE(App app) {
        super(app);
    }

    public void setDownstream(Stream<Event> stream) {
        this.downStream = stream;
    }

    public void onEvent(Event event) {
        if (firstEvent) {
            logger.info("Handling new topic [{}]", getId());
            firstEvent = false;
        }
        count += event.get("count", Integer.class);
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void onTime() {
        if (count < threshold) {
            return;
        }
        Event topicSeenEvent = new Event();
        topicSeenEvent.put("topic", String.class, getId());
        topicSeenEvent.put("count", Integer.class, count);
        topicSeenEvent.put("aggregationKey", String.class, "aggregationValue");
        downStream.put(topicSeenEvent);
    }

    @Override
    protected void onRemove() {
    }

}
