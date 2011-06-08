package io.s4;

import java.util.List;

public abstract class App {
		
	private List<ProcessingElement> processingElements;
	
	protected abstract void create();
	
	private void createInternal() {
		
		// create something;
		
		// create concrete class
		create();
	}
	
	protected abstract void init();

	private void initInternal() {
		
		// init something
		
		// init concrete class
		init();
		
	}
	
	void addProcessingElement(ProcessingElement pe) {
		
		processingElements.add(pe);
		
	}
}
