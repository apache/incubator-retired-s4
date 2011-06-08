package io.s4;

import java.util.List;

public abstract class ProcessingElement {

	final private App app;
	private List<Stream> inputStreams;
	private List<Stream> outputStreams;

	public ProcessingElement(App app) {
	
		this.app = app;
		app.addProcessingElement(this);
	}
	
	
	public ProcessingElement setInput(Stream stream) {

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


	abstract public void processInputEvent(Event event);

	abstract public void sendOutputEvent();

	abstract public void init();

}
