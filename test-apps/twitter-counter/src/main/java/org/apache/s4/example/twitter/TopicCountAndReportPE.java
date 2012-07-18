package org.apache.s4.example.twitter;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// keyed by topic name
public class TopicCountAndReportPE extends ProcessingElement {

    transient Stream<TopicEvent> downStream;
    transient int threshold = 10;
    int count;
    boolean firstEvent = true;

    static Logger logger = LoggerFactory.getLogger(TopicCountAndReportPE.class);

    public TopicCountAndReportPE() {
        // required for checkpointing in S4 0.5. Requirement to be removed in 0.6
    }

    public TopicCountAndReportPE(App app) {
        super(app);
    }

    public void setDownstream(Stream<TopicEvent> aggregatedTopicStream) {
        this.downStream = aggregatedTopicStream;
    }

    public void onEvent(TopicEvent event) {
        if (firstEvent) {
            logger.info("Handling new topic [{}]", getId());
            firstEvent = false;
        }
        count += event.getCount();
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void onTime() {
        if (count < threshold) {
            return;
        }
        // Event topicSeenEvent = new Event();
        // topicSeenEvent.put("topic", String.class, getId());
        // topicSeenEvent.put("count", Integer.class, count);
        // topicSeenEvent.put("aggregationKey", String.class, "aggregationValue");
        downStream.put(new TopicEvent(getId(), count));
    }

    @Override
    protected void onRemove() {
    }

}
