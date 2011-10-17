package io.s4.wordcount;

import io.s4.core.KeyFinder;

import java.util.ArrayList;
import java.util.List;

public class WordSeenKeyFinder implements KeyFinder<WordSeenEvent> {

    @Override
    public List<String> get(WordSeenEvent event) {
        List<String> key = new ArrayList<String>();
        key.add(event.getWord());
        return key;
    }

}
