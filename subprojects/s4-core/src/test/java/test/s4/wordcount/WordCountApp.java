package test.s4.wordcount;


import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import test.s4.fixtures.SocketAdapter;

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

        WordClassifierPE wordClassifierPrototype = createPE(WordClassifierPE.class);
        Stream<WordCountEvent> wordCountStream = createStream("words counts stream", new WordCountKeyFinder(),
                wordClassifierPrototype);
        WordCounterPE wordCounterPrototype = createPE(WordCounterPE.class);
//        wordCounterPrototype.setTrigger(WordSeenEvent.class, 1, 0, null);
        wordCounterPrototype.setWordClassifierStream(wordCountStream);
        Stream<WordSeenEvent> wordSeenStream = createStream("words seen stream", new WordSeenKeyFinder(),
                wordCounterPrototype);
        WordSplitterPE wordSplitterPrototype = createPE(WordSplitterPE.class);
        wordSplitterPrototype.setWordSeenStream(wordSeenStream);
        Stream<StringEvent> sentenceStream = createStream("sentences stream", new SentenceKeyFinder(),
                wordSplitterPrototype);

        try {
            socketAdapter = new SocketAdapter<StringEvent>(sentenceStream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (checkpointing) {

            // TODO move to subclass

            // checkpoint word classifier because it maintains a sync counter
            // for the test
            // LoggerFactory.getLogger(getClass()).info("setting checkpointing for word classifier and word counter");
            // wordClassifierPrototype.setCheckpointingFrequency(1);
            // // checkpoint word counter because it maintains word counts
            // wordCounterPrototype.setCheckpointingFrequency(1);
        }

    }

    @Override
    protected void close() {
        socketAdapter.close();

    }

}
