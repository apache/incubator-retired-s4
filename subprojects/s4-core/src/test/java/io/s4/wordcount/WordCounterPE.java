package io.s4.wordcount;

import io.s4.core.App;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;

public class WordCounterPE extends ProcessingElement {
    
    int wordCounter;
    transient Stream<WordCountEvent> wordClassifierStream;

    private WordCounterPE() {}
    
    public WordCounterPE(App app, Stream<WordCountEvent> wordClassifierStream) {
        super(app);
        this.wordClassifierStream = wordClassifierStream;
    }
    
    public void processInputEvent(WordSeenEvent event) { 
        wordCounter++;
        System.out.println("seen word " + event.getWord());
        // NOTE: it seems the id is the key for now...     
        wordClassifierStream.put(new WordCountEvent(getId(), wordCounter));
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
