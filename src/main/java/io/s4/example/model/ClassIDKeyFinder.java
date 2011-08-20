package io.s4.example.model;

import java.util.ArrayList;
import java.util.List;

import io.s4.KeyFinder;

public class ClassIDKeyFinder implements
		KeyFinder<ObsEvent> {

	@Override
	public List<String> get(ObsEvent event) {
		
		List<String> results = new ArrayList<String>();
        
        /* Retrieve the user ID and add it to the list. */
        results.add(Integer.toString(event.getClassId()));
        
        return results;   
	}

}
