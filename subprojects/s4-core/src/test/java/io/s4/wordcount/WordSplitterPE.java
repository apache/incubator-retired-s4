package io.s4.wordcount;

import io.s4.core.App;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;


public class WordSplitterPE extends ProcessingElement {
    
    final private Stream<WordSeenEvent> wordSeenStream;

    public WordSplitterPE(App app, Stream<WordSeenEvent> wordSeenStream) {
        super(app);
        this.wordSeenStream = wordSeenStream;
    }

    public void processInputEvent(StringEvent event) {
        StringEvent sentence = event;
        String[] split = sentence.getString().split(" ");
        for (String word : split) {
            wordSeenStream.put(new WordSeenEvent(word));
        }
        
        
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
