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
import io.s4.core.Event;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;

public class ModelPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ModelPE.class);

    final private int vectorSize;
    final private long numVectors;
    final private int numClasses;
    private Stream<ObsEvent> distanceStream;
    private int modelId;
    private float[] centroid;
    private long obsCount = 0;
    private float[] obsSum;
    private long totalCount = 0;
    private int[] confusionRow;

    public ModelPE(App app, int vectorSize, long numVectors, int numClasses) {
        super(app);
        this.vectorSize = vectorSize;
        this.numVectors = numVectors;
        this.numClasses = numClasses;
    }

    public void setStream(Stream<ObsEvent> distanceStream) {

        /* Init prototype. */
        this.distanceStream = distanceStream;
    }

    public long getObsCount() {
        return obsCount;
    }

    private void updateStats(ObsEvent event) {

        logger.trace("TRAINING: ModelID: {}, {}", modelId, event.toString());

        float[] obs = event.getObsVector();
        for (int i = 0; i < vectorSize; i++) {
            obsSum[i] += obs[i];
        }

        obsCount++;

        /* Log info. */
        if (obsCount % 1000 == 0) {
            logger.info("Trained model using {} events with class id {}",
                    obsCount, modelId);
        }
    }

    /*
     * Compute Euclidean distance between an observed vectors and the centroid.
     */
    private float distance(float[] obs) {

        float sumSq = 0f;
        for (int i = 0; i < vectorSize; i++) {
            float diff = centroid[i] - obs[i];
            sumSq += diff * diff;
        }
        return (float) Math.sqrt(sumSq);
    }

    private void updateModel() {

        for (int i = 0; i < vectorSize; i++) {
            centroid[i] = obsSum[i] / obsCount;
            obsSum[i] = 0f;
        }

        /* Print mean vector. */
        StringBuilder vector = new StringBuilder();
        for (int i = 0; i < vectorSize; i++) {
            vector.append(centroid[i] + " ");
        }
        logger.info("Update mean for model {} is: {}", modelId, vector);

        obsCount = 0;
        totalCount = 0;
    }

    private void updateResults(ObsEvent event) {
        confusionRow[event.getHypId()] += 1;
        obsCount++;
    }

    /*
     * 
     * @see io.s4.ProcessingElement#processInputEvent(io.s4.Event)
     * 
     * Read input event, compute distance to current centroid and emit.
     * 
     * All models receive an end of training stream marker.
     */
    @Override
    protected void processInputEvent(Event event) {

        ObsEvent inEvent = (ObsEvent) event;
        float[] obs = inEvent.getObsVector();

        /* Estimate model parameters using the training data. */
        if (inEvent.isTraining()) {

            /*
             * Ignore events with negative index. They are just used to create
             * the PE.
             */
            if (inEvent.getIndex() < 0) {
                return;
            }

            if (++totalCount == numVectors) {

                /* End of training stream. */
                updateModel();

                /* Could send ack here. */

                return;
            }

            /* Check if the event belongs to this class. */
            if (inEvent.getClassId() == modelId) {

                updateStats(inEvent);

            } else {

                /* Not needed to compute the mean vector. */
                return;
            }

        } else { // scoring

            if (inEvent.getHypId() < 0) {
                /* Score observed vector and send it to the minimizer. */

                float dist = distance(obs);
                ObsEvent outEvent = new ObsEvent(inEvent.getIndex(), obs, dist,
                        inEvent.getClassId(), modelId, false);

                distanceStream.put(outEvent);

            } else {

                /* Got the hypothesis. */
                updateResults(inEvent);
            }
        }
    }

    @Override
    public void processOutputEvent(Event event) {

        ObsEvent inEvent = (ObsEvent) event;

        if (inEvent.isTraining() || inEvent.getHypId() < 0)
            return;

        String s = String
                .format("RESULTS for model %2d:", inEvent.getClassId());
        StringBuilder sb = new StringBuilder(s);
        for (int i = 0; i < numClasses; i++) {
            float pct = (float) confusionRow[i] / (float) obsCount * 100f;
            sb.append(String.format("%6.1f", pct));
        }
        sb.append(String.format(" Count:  %6d", obsCount));
        logger.info(sb.toString());
    }

    @Override
    protected void onCreate() {

        this.modelId = Integer.parseInt(id);

        /* Create an array for each PE instance. */
        this.obsSum = new float[vectorSize];
        this.centroid = new float[vectorSize]; // we could do sum in place but
                                               // for now lets use two vectors.
        this.confusionRow = new int[numClasses];
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
