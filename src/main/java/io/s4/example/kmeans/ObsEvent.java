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
package io.s4.example.kmeans;

import io.s4.Event;

public class ObsEvent extends Event {

	final private float[] obsVector;
	final private float distance;
	final private long index;
	final private int classId;
	
	public ObsEvent(long index, float[] obsVector, float distance, int classId) {
		this.obsVector = obsVector;
		this.distance = distance;
		this.index = index;
		this.classId = classId;
	}

	public float[] getObsVector() {
		return obsVector;
	}

	public float getDistance() {
		return distance;
	}

	public long getIndex() {
		return index;
	}
	
	public int getClassId() {
		return classId;
	}
	
	public String toString() {
		
		StringBuilder vector = new StringBuilder();;
		for(int i=0; i < obsVector.length; i++) {
			vector.append(obsVector[i] + " ");
		}
        return "Idx: " + index + ", Label: " + classId + ", Dist: " + distance + ", Obs: " + vector.toString();
    }
}
