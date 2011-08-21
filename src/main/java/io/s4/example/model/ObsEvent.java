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

import io.s4.Event;

public class ObsEvent extends Event {

    final private float[] obsVector;
    final private float distance;
    final private long index;
    final private int classId;
    final private int hypId;
    final private boolean isTraining;

    public ObsEvent(long index, float[] obsVector, float distance, int classId,
            int hypId, boolean isTraining) {
        this.obsVector = obsVector;
        this.distance = distance;
        this.index = index;
        this.classId = classId;
        this.hypId = hypId;
        this.isTraining = isTraining;
    }

    /**
     * @return the observed data vector.
     */
    public float[] getObsVector() {
        return obsVector;
    }

    /**
     * @return the distance between the observed vector and the class centroid.
     *         Use -1.0 when unknown.
     */
    public float getDistance() {
        return distance;
    }

    /**
     * @return the index of the data vector.
     */
    public long getIndex() {
        return index;
    }

    /**
     * @return the true class of the vector.
     */
    public int getClassId() {
        return classId;
    }

    /**
     * @return the hypothesized class of the vector. Use -1 when unknown.
     */
    public int getHypId() {
        return hypId;
    }

    /**
     * @return true if this is training data.
     */
    public boolean isTraining() {
        return isTraining;
    }

    public String toString() {

        StringBuilder vector = new StringBuilder();
        for (int i = 0; i < obsVector.length; i++) {
            vector.append(obsVector[i] + " ");
        }
        return "Idx: " + index + ", Class: " + classId + ", Hyp:" + hypId
                + ", Dist: " + distance + ", isTraining: " + isTraining
                + ", Obs: " + vector.toString();
    }

}
