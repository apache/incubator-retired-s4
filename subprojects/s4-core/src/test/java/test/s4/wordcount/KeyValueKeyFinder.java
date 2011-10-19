package test.s4.wordcount;


import java.util.Arrays;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class KeyValueKeyFinder implements KeyFinder<KeyValueEvent> {

    public static final String UNIQUE_KEY = "KEY";

    @Override
    public List<String> get(final KeyValueEvent event) {
        return Arrays.asList(new String[] {UNIQUE_KEY});
    }

}
