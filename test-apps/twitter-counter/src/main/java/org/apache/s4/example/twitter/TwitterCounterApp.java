package org.apache.s4.example.twitter;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.core.ft.CheckpointingConfig;
import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;

import com.google.common.collect.ImmutableList;

public class TwitterCounterApp extends App {

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        try {

            TopNTopicPE topNTopicPE = createPE(TopNTopicPE.class);
            topNTopicPE.setTimerInterval(10, TimeUnit.SECONDS);
            // we checkpoint this PE every 20s
            topNTopicPE.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.TIME).frequency(20)
                    .timeUnit(TimeUnit.SECONDS).build());
            @SuppressWarnings("unchecked")
            Stream<TopicEvent> aggregatedTopicStream = createStream("AggregatedTopicSeen", new KeyFinder<TopicEvent>() {

                @Override
                public List<String> get(final TopicEvent arg0) {
                    return ImmutableList.of("aggregationKey");
                }
            }, topNTopicPE);

            TopicCountAndReportPE topicCountAndReportPE = createPE(TopicCountAndReportPE.class);
            topicCountAndReportPE.setDownstream(aggregatedTopicStream);
            topicCountAndReportPE.setTimerInterval(10, TimeUnit.SECONDS);
            // we checkpoint instances every 2 events
            topicCountAndReportPE.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.EVENT_COUNT)
                    .frequency(2).build());
            Stream<TopicEvent> topicSeenStream = createStream("TopicSeen", new KeyFinder<TopicEvent>() {

                @Override
                public List<String> get(final TopicEvent arg0) {
                    return ImmutableList.of(arg0.getTopic());
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
