package org.apache.s4.wordcount;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import com.google.inject.Inject;

public class WordCountApp extends App {

    protected boolean checkpointing = false;

    @Inject
    public WordCountApp() {
        super();
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {

        WordClassifierPE wordClassifierPrototype = createPE(WordClassifierPE.class, "classifierPE");
        Stream<WordCountEvent> wordCountStream = createStream("words counts stream", new WordCountKeyFinder(),
                wordClassifierPrototype);
        WordCounterPE wordCounterPrototype = createPE(WordCounterPE.class, "counterPE");
        // wordCounterPrototype.setTrigger(WordSeenEvent.class, 1, 0, null);
        wordCounterPrototype.setWordClassifierStream(wordCountStream);
        Stream<WordSeenEvent> wordSeenStream = createStream("words seen stream", new WordSeenKeyFinder(),
                wordCounterPrototype);
        WordSplitterPE wordSplitterPrototype = createPE(WordSplitterPE.class);
        wordSplitterPrototype.setWordSeenStream(wordSeenStream);
        Stream<Event> sentenceStream = createInputStream("inputStream", new SentenceKeyFinder(), wordSplitterPrototype);
    }

    @Override
    protected void onClose() {

    }

}
