package org.apache.s4.core.triggers;

import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.core.TriggerTest;
import org.apache.s4.wordcount.SentenceKeyFinder;

import com.google.inject.Inject;

public class TriggeredApp extends App {

    public Stream<Event> stream;

    @Inject
    public TriggeredApp() {
        super();
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {

        TriggerablePE prototype = createPE(TriggerablePE.class);
        stream = createStream("stream", new SentenceKeyFinder(), prototype);
        switch (TriggerTest.triggerType) {
            case COUNT_BASED:
                prototype.setTrigger(Event.class, 1, 0, TimeUnit.SECONDS);
                break;
            case TIME_BASED:
                prototype.setTrigger(Event.class, 1, 1, TimeUnit.MILLISECONDS);
            default:
                break;
        }

    }

    @Override
    protected void onClose() {
    }

}
