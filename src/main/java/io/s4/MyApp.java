package io.s4;

public class MyApp extends App {

	final private Stream s1;
	final private Stream s2;
	final private ProcessingElement pe1;
	final private ProcessingElement pe2;
	
	public MyApp(Stream s1, Stream s2, ProcessingElement pe1,
			ProcessingElement pe2) {
		super();
		this.s1 = s1;
		this.s2 = s2;
		this.pe1 = pe1;
		this.pe2 = pe2;
	}

	@Override
	protected void create() {
		
		pe1.setOutput(s1);
		pe2.setInput(s1);
		pe2.setOutput(s2);
	}

	@Override
	protected void init() {
		// TODO Auto-generated method stub

	}
	
}
