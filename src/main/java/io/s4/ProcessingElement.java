package io.s4;

import java.util.List;

import com.google.inject.Inject;

public abstract class ProcessingElement {

	final private App app;
	private List<Stream> inputStreams;
	private List<Stream> outputStreams;

	@Inject
	public ProcessingElement(App app) {
	
		this.app = app;
		app.addProcessingElement(this);
	}
	
	
	public ProcessingElement setInput(Stream stream, Key key) {

		inputStreams.add(stream);
		
		return this;
	}

	public ProcessingElement setOutput(Stream stream) {

		outputStreams.add(stream);
		
		return this;
	}

	
	/**
	 * @return the app
	 */
	public App getApp() {
		return app;
	}


	 public void processInputEvent(Event event) {
	     
	     // map event event_type to processInputEvent(EVENT_TYPE)
	     
	     // the method gets auto-generated
	 }

	abstract public void sendOutputEvent();

	abstract public void init();
	
	// TODO: Change equals and hashCode in ProcessingElement and 
	// Stream so we can use sets as collection and make sure there are no duplicate prototypes. 
	// Great article: http://www.artima.com/lejava/articles/equality.html
	

}
