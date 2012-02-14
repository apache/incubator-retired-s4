package org.apache.s4.edsl;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.base.KeyFinder;

public class DurationKeyFinder implements KeyFinder<EventA> {

    public List<String> get(EventA event) {

        List<String> results = new ArrayList<String>();

        /* Retrieve the gender and add it to the list. */
        results.add(Long.toString(event.getDuration()));

        return results;
    }
}
