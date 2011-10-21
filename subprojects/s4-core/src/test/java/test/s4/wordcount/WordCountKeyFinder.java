package test.s4.wordcount;


import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class WordCountKeyFinder implements KeyFinder<WordCountEvent> {

    @SuppressWarnings("serial")
    @Override
    public List<String> get(WordCountEvent event) {
        return new ArrayList<String>(){{add("classifier");}};
    }

}
