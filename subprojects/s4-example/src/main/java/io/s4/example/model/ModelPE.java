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

import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.core.App;
import io.s4.base.Event;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;
import io.s4.model.Model;

@ThreadSafe
final public class ModelPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ModelPE.class);

    private long numVectors;
    private Model model;
    private Stream<ObsEvent> distanceStream;
    private Stream<ResultEvent> resultStream;
    private int modelId;
    private double logPriorProb;
    private long obsCount = 0;
    private long totalCount = 0;
    private int iteration = 0;

    public ModelPE(App app) {
        super(app);
    }

    /**
     * @param numVectors the numVectors to set
     */
    public void setNumVectors(long numVectors) {
        this.numVectors = numVectors;
    }

    /**
     * @return the number of training vectors.
     */
    public long getNumVectors() {
        return numVectors;
    }

    /**
     * @param model
     *            the model to set
     */
    public void setModel(Model model) {
        this.model = model;
    }

    /**
     * @return the model
     */
    public Model getModel() {
        return model;
    }

    /**
     * Set the output streams.
     * 
     * @param distanceStream
     *            sends an {@link ObsEvent} to the {@link MaximizerPE}.
     * @param resultStream
     *            sends a {@link ResultEvent} to the {@link MetricsPE}.
     */
    public void setStream(Stream<ObsEvent> distanceStream,
            Stream<ResultEvent> resultStream) {

        /* Init prototype. */
        this.distanceStream = distanceStream;
        this.resultStream = resultStream;
    }

    /**
     * @return number of observation vectors used in training iteration.
     */
    public long getObsCount() {
        return obsCount;
    }

    /**
     * @return current iteration.
     */
    public int getIteration() {
        return iteration;
    }

    private void updateStats(ObsEvent event) {

        logger.trace("TRAINING: ModelID: {}, {}", modelId, event.toString());
        model.update(event.getObsVector());

        obsCount++;

        /* Log info. */
        if (obsCount % 10000 == 0) {
            logger.info("Trained model using {} events with class id {}",
                    obsCount, modelId);
        }
    }

    private void estimateModel() {

        model.estimate();

        double prob = (double) obsCount / numVectors;
        logPriorProb = Math.log(prob);
        logger.info("Prior prob: {}", prob);
        logger.info("Update params for model {} is: {}", modelId,
                model.toString());

        obsCount = 0;
        totalCount = 0;
        model.clearStatistics();

        /* Ready to start next iteration. */
        iteration++;
    }

    public void onEvent(Event event) {

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
                estimateModel();

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
                /* Score observed vector and send it to the maximizer. */
                float dist = (float) (model.logProb(obs) + logPriorProb);
                ObsEvent outEvent = new ObsEvent(inEvent.getIndex(), obs, dist,
                        inEvent.getClassId(), modelId, false);

                logger.trace(inEvent.getIndex() + " " + inEvent.getClassId()
                        + " " + modelId + " " + model.logProb(obs) + " "
                        + logPriorProb + " " + dist);

                distanceStream.put(outEvent);

            } else {

                /* Send out result. */
                if (resultStream != null) {
                    ResultEvent resultEvent = new ResultEvent(
                            inEvent.getIndex(), inEvent.getClassId(),
                            inEvent.getHypId());

                    resultStream.put(resultEvent);
                }
            }
        }
    }

    @Override
    protected void onCreate() {

        this.modelId = Integer.parseInt(getId());

        /*
         * Initialize model. When a new PE instance is created we use the
         * reference to the model in the PE prototype (initial value in variable
         * model) to create a new model for this PE instance (final value in
         * variable model).
         */
        model = model.create();
    }

    @Override
    protected void onRemove() {

    }
}
