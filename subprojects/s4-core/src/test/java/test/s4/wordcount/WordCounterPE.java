package test.s4.wordcount;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class WordCounterPE extends ProcessingElement {
    
    int wordCounter;
    transient Stream<WordCountEvent> wordClassifierStream;

    private WordCounterPE() {}
    
    public WordCounterPE(App app) {
        super(app);
    }
    
    public void setWordClassifierStream(Stream<WordCountEvent> stream) {
        this.wordClassifierStream = stream;
    }

    public void onEvent(WordSeenEvent event) { 
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
