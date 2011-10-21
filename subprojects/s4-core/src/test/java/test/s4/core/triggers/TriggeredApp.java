package test.s4.core.triggers;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import test.s4.fixtures.SocketAdapter;
import test.s4.wordcount.SentenceKeyFinder;
import test.s4.wordcount.StringEvent;

import com.google.inject.Inject;

public class TriggeredApp extends App {

    SocketAdapter<StringEvent> socketAdapter;

    @Inject
    public TriggeredApp() {
        super();
    }

    @Override
    protected void start() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void init() {

        TriggerablePE prototype = createPE(TriggerablePE.class);
        Stream<StringEvent> stream = createStream("stream", new SentenceKeyFinder(), prototype);
        switch (TriggerTest.triggerType) {
            case COUNT_BASED:
                prototype.setTrigger(Event.class, 1, 0, TimeUnit.SECONDS);
                break;
            case TIME_BASED:
                prototype.setTrigger(Event.class, 1, 1, TimeUnit.MILLISECONDS);
            default:
                break;
        }

        try {
            socketAdapter = new SocketAdapter<StringEvent>(stream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void close() {
    }

}
