package org.apache.s4.example.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;


public class ObsIndexKeyFinder implements KeyFinder<ObsEvent> {

    @Override
    public List<String> get(ObsEvent event) {

        List<String> results = new ArrayList<String>();

        /* Retrieve the user ID and add it to the list. */
        results.add(Long.toString(event.getIndex()));

        return results;
    }

}
