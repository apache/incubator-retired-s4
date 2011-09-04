package io.s4.example.model;

import java.util.ArrayList;
import java.util.List;

import io.s4.core.KeyFinder;

public class ResultKeyFinder implements KeyFinder<ResultEvent> {

    @Override
    public List<String> get(ResultEvent event) {
        
        List<String> results = new ArrayList<String>();

        /* Retrieve the user ID and add it to the list. */
        results.add("1234");

        return results;
    }

}
