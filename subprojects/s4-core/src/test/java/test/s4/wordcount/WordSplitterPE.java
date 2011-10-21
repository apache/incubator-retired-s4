package test.s4.wordcount;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;


public class WordSplitterPE extends ProcessingElement {
    
    private Stream<WordSeenEvent> wordSeenStream;

    public WordSplitterPE(App app) {
        super(app);
    }

    public void onEvent(StringEvent event) {
        StringEvent sentence = event;
        String[] split = sentence.getString().split(" ");
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
