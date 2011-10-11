package io.s4.wordcount;

import io.s4.core.KeyFinder;

import java.util.ArrayList;
import java.util.List;

public class WordCountKeyFinder implements KeyFinder<WordCountEvent> {

    @SuppressWarnings("serial")
    @Override
    public List<String> get(WordCountEvent event) {
        return new ArrayList<String>(){{add("classifier");}};
    }

}
