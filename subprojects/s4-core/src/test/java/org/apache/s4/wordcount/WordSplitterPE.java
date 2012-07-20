package org.apache.s4.wordcount;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class WordSplitterPE extends ProcessingElement {

    private Stream<WordSeenEvent> wordSeenStream;

    public WordSplitterPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {
        String[] split = event.get("sentence").split(" ");
        for (String word : split) {
            wordSeenStream.put(new WordSeenEvent(word));
        }
    }

    public void setWordSeenStream(Stream<WordSeenEvent> stream) {
        this.wordSeenStream = stream;
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
