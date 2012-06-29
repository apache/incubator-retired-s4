package org.apache.s4.edsl.wordcount;

import java.io.IOException;

import org.apache.s4.core.Stream;
import org.apache.s4.edsl.BuilderS4DSL;
import org.apache.s4.fixtures.SocketAdapter;
import org.apache.s4.wordcount.SentenceKeyFinder;
import org.apache.s4.wordcount.StringEvent;
import org.apache.s4.wordcount.WordClassifierPE;
import org.apache.s4.wordcount.WordCountEvent;
import org.apache.s4.wordcount.WordCountKeyFinder;
import org.apache.s4.wordcount.WordCounterPE;
import org.apache.s4.wordcount.WordSeenEvent;
import org.apache.s4.wordcount.WordSeenKeyFinder;
import org.apache.s4.wordcount.WordSplitterPE;

public class WordCountApp extends BuilderS4DSL {

    SocketAdapter<StringEvent> socketAdapter;

    protected void onInit() {
        pe("Classifier").type(WordClassifierPE.class).pe("Counter").type(WordCounterPE.class)
                .emit(WordCountEvent.class).withKeyFinder(WordCountKeyFinder.class).to("Classifier").pe("Splitter")
                .type(WordSplitterPE.class).emit(WordSeenEvent.class).withKeyFinder(WordSeenKeyFinder.class)
                .to("Counter").build();
        Stream<StringEvent> sentenceStream = createStream("sentences stream", new SentenceKeyFinder(),
                getPE("Splitter"));
        try {
            socketAdapter = new SocketAdapter<StringEvent>(sentenceStream, new SocketAdapter.SentenceEventFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
