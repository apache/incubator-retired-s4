package test.s4.wordcount;


import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class WordSeenKeyFinder implements KeyFinder<WordSeenEvent> {

    @Override
    public List<String> get(WordSeenEvent event) {
        List<String> key = new ArrayList<String>();
        key.add(event.getWord());
        return key;
    }

}
