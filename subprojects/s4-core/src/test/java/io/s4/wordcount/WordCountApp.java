package io.s4.wordcount;

import io.s4.SocketAdapter;
import io.s4.core.App;
import io.s4.core.Stream;

import java.io.IOException;

import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class WordCountApp extends App {

    protected boolean checkpointing = false;
    SocketAdapter<StringEvent> socketAdapter;

    @Inject
    public WordCountApp() {
        super();
    }

    @Override
    protected void start() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void init() {

        WordClassifierPE wordClassifierPrototype = new WordClassifierPE(this);
        Stream<WordCountEvent> wordCountStream = new Stream<WordCountEvent>(this, "words counts stream",
                new WordCountKeyFinder(), wordClassifierPrototype);
        WordCounterPE wordCounterPrototype = new WordCounterPE(this, wordCountStream);
        Stream<WordSeenEvent> wordSeenStream = new Stream<WordSeenEvent>(this, "words seen stream",
                new WordSeenKeyFinder(), wordCounterPrototype);
        WordSplitterPE wordSplitterPrototype = new WordSplitterPE(this, wordSeenStream);
        Stream<StringEvent> sentenceStream = new Stream<StringEvent>(this, "sentences stream",
                new SentenceKeyFinder(), wordSplitterPrototype);

        try {
            socketAdapter = new SocketAdapter<StringEvent>(sentenceStream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (checkpointing) {
            
            // TODO move to subclass
            
            // checkpoint word classifier because it maintains a sync counter
            // for the test
//            LoggerFactory.getLogger(getClass()).info("setting checkpointing for word classifier and word counter");
//            wordClassifierPrototype.setCheckpointingFrequency(1);
//            // checkpoint word counter because it maintains word counts
//            wordCounterPrototype.setCheckpointingFrequency(1);
        }

    }


    @Override
    protected void close() {
        socketAdapter.close();

    }

}
