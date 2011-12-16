package org.apache.s4.fluent;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class HeightKeyFinder implements KeyFinder<EventA> {

    public List<String> get(EventA event) {

        List<String> results = new ArrayList<String>();

        /* Retrieve the gender and add it to the list. */
        results.add(Integer.toString(event.getHeight()));

        return results;
    }
}
