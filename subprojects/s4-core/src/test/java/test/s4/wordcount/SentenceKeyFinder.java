package test.s4.wordcount;


import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class SentenceKeyFinder implements KeyFinder<StringEvent> {

    private static final String SENTENCE_KEY = "sentence";

    @SuppressWarnings("serial")
    @Override
    public List<String> get(StringEvent event) {
        return new ArrayList<String>(){{add(SENTENCE_KEY);}};
    }

}
