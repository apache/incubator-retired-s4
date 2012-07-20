package org.apache.s4.wordcount;

import java.util.List;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;

import com.google.common.collect.ImmutableList;

public class SentenceKeyFinder implements KeyFinder<Event> {

    private static final String SENTENCE_KEY = "sentence";

    @Override
    public List<String> get(Event event) {
        return ImmutableList.of(SENTENCE_KEY);
    }

}
