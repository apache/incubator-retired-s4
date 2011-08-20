/*
 * Copyright (c) 2011 The S4 Project, http://s4.io.
 * All rights reserved.
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
package io.s4.example.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.App;
import io.s4.Event;
import io.s4.ProcessingElement;
import io.s4.Stream;

public class MinimizerPE extends ProcessingElement {

	Logger logger = LoggerFactory.getLogger(MinimizerPE.class);

	final private int numClasses;
	final private Stream<ObsEvent> assignmentStream;
	private int numEventsReceived = 0;
	private float minDistance = Float.MAX_VALUE;
	private int hypID;

	public MinimizerPE(App app, int numClusters, Stream<ObsEvent> assignmentStream) {
		super(app);
		this.numClasses = numClusters;
		this.assignmentStream = assignmentStream;
	}

	@Override
	protected void processInputEvent(Event event) {

		ObsEvent inEvent = (ObsEvent) event;
		float[] obs = inEvent.getObsVector();
		
		if(inEvent.getDistance() < minDistance) {
			minDistance = inEvent.getDistance();
			hypID = inEvent.getHypId();
		}
		
		if( ++numEventsReceived == numClasses) {
			
			/* Got all the distances. Send class id with minimum distance. */
			ObsEvent outEvent = new ObsEvent(inEvent.getIndex(), obs,
					minDistance, inEvent.getClassId(), hypID, false);
			
			logger.trace("IN: " + inEvent.toString());
			logger.trace("OUT: " + outEvent.toString());
			
			assignmentStream.put(outEvent);
			
			/*  This PE instance is no longer needed. */
			close();
		}
		
		
	}

	@Override
	public void sendEvent() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeInstanceForKey(String id) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void initPEInstance() {
		// TODO Auto-generated method stub
		
	}

}
