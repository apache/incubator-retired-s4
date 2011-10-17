package io.s4.wordcount;

import io.s4.core.KeyFinder;

import java.util.Arrays;
import java.util.List;

public class KeyValueKeyFinder implements KeyFinder<KeyValueEvent> {

    public static final String UNIQUE_KEY = "KEY";

    @Override
    public List<String> get(final KeyValueEvent event) {
        return Arrays.asList(new String[] {UNIQUE_KEY});
    }

}
