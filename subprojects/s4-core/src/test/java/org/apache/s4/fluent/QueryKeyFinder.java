package org.apache.s4.fluent;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;

public class QueryKeyFinder implements KeyFinder<EventB> {

    public List<String> get(EventB event) {

        List<String> results = new ArrayList<String>();

        /* Retrieve the gender and add it to the list. */
        results.add(event.getQuery());

        return results;
    }
}
