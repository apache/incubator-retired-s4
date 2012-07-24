/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.example.model;

import org.apache.s4.base.Event;

import net.jcip.annotations.Immutable;

@Immutable
final public class ObsEvent extends Event {

    private float[] obsVector;
    private float prob;
    private long index;
    private int classId;
    private int hypId;
    private boolean isTraining;
    
    public ObsEvent() {
        
    }

    public ObsEvent(long index, float[] obsVector, float prob, int classId,
            int hypId, boolean isTraining) {
        this.obsVector = obsVector;
        this.prob = prob;
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
     * @return the probability of this event.
     */
    public float getProb() {
        return prob;
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
                + ", Prob: " + prob + ", isTraining: " + isTraining
                + ", Obs: " + vector.toString();
    }

}
