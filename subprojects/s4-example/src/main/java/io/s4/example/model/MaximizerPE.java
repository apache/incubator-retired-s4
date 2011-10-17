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

import io.s4.core.App;
import io.s4.base.Event;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;

final public class MaximizerPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory
            .getLogger(MaximizerPE.class);

    private int numClasses;
    private Stream<ObsEvent> assignmentStream;
    private int numEventsReceived = 0;
    private float maxLogProb = -Float.MAX_VALUE;
    private int hypID;

    public MaximizerPE(App app) {
        super(app);
    }

    /**
     * @param countStream
     *            the countStream to set
     */
    public void setAssignmentStream(Stream<ObsEvent> stream) {
        assignmentStream = stream;
    }

    /**
     * @return the numClasses
     */
    public int getNumClasses() {
        return numClasses;
    }

    /**
     * @param numClasses
     *            the numClasses to set
     */
    public void setNumClasses(int numClasses) {
        this.numClasses = numClasses;
    }

    public void onEvent(Event event) {

        ObsEvent inEvent = (ObsEvent) event;
        float[] obs = inEvent.getObsVector();

        if (inEvent.getProb() > maxLogProb) {
            maxLogProb = inEvent.getProb();
            hypID = inEvent.getHypId();
        }

        if (++numEventsReceived == numClasses) {

            /* Got all the distances. Send class id with minimum distance. */
            ObsEvent outEvent = new ObsEvent(inEvent.getIndex(), obs,
                    maxLogProb, inEvent.getClassId(), hypID, false);

            logger.trace("IN: " + inEvent.toString());
            logger.trace("OUT: " + outEvent.toString());

            assignmentStream.put(outEvent);

            /* This PE instance is no longer needed. */
            close();
        }

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

}
