package org.apache.s4.core;

import com.google.inject.Injector;

public interface Bootstrap {
	void start(Injector parentInjector) throws Exception;
}
