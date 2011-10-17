package io.s4.wordcount;

import io.s4.core.KeyFinder;

import java.util.ArrayList;
import java.util.List;

public class SentenceKeyFinder implements KeyFinder<StringEvent> {

    private static final String SENTENCE_KEY = "sentence";

    @SuppressWarnings("serial")
    @Override
    public List<String> get(StringEvent event) {
        return new ArrayList<String>(){{add(SENTENCE_KEY);}};
    }

}
