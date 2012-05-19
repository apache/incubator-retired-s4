package org.apache.s4.example.twitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

public class TwitterCounterApp extends App {

    private ZkClient zkClient;

    private Thread t;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {

            TopNTopicPE topNTopicPE = createPE(TopNTopicPE.class);
            topNTopicPE.setTimerInterval(10, TimeUnit.SECONDS);
            @SuppressWarnings("unchecked")
            Stream<TopicEvent> aggregatedTopicStream = createStream("AggregatedTopicSeen", new KeyFinder<TopicEvent>() {

                @Override
                public List<String> get(final TopicEvent arg0) {
                    return new ArrayList<String>() {
                        {
                            add("aggregationKey");
                        }
                    };
                }
            }, topNTopicPE);

            TopicCountAndReportPE topicCountAndReportPE = createPE(TopicCountAndReportPE.class);
            topicCountAndReportPE.setDownstream(aggregatedTopicStream);
            topicCountAndReportPE.setTimerInterval(10, TimeUnit.SECONDS);
            Stream<TopicEvent> topicSeenStream = createStream("TopicSeen", new KeyFinder<TopicEvent>() {

                @Override
                public List<String> get(final TopicEvent arg0) {
                    return new ArrayList<String>() {
                        {
                            add(arg0.getTopic());
                        }
                    };
                }
            }, topicCountAndReportPE);

            TopicExtractorPE topicExtractorPE = createPE(TopicExtractorPE.class);
            topicExtractorPE.setDownStream(topicSeenStream);
            topicExtractorPE.setSingleton(true);
            createInputStream("RawStatus", topicExtractorPE);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onStart() {

    }
}
