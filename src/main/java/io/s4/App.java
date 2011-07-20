/*
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4;

import java.util.ArrayList;
import java.util.List;

public abstract class App {
		
	private List<ProcessingElement> pePrototypes = new ArrayList<ProcessingElement>();
	
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
	
	void addPEPrototype(ProcessingElement pePrototype) {
		
		pePrototypes.add(pePrototype);
		
	}
}
