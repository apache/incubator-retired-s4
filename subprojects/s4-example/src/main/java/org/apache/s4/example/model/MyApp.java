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
package org.apache.s4.example.model;

import java.util.concurrent.TimeUnit;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.apache.s4.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyApp extends App {

    private static final Logger logger = LoggerFactory.getLogger(MyApp.class);

    final private int numClasses;
    final private long numVectors;
    final private int outputInterval;
    final private TimeUnit timeUnit;
    final private Model model;
    private Stream<ObsEvent> obsStream;

    private ModelPE modelPE;
    private MetricsPE metricsPE;
    Stream<ObsEvent> assignmentStream;
    private String report;

    MyApp(int numClasses, long numVectors, Model model, int outputInterval, TimeUnit timeUnit) {
        super();
        this.numClasses = numClasses;
        this.numVectors = numVectors;
        this.model = model;
        this.outputInterval = outputInterval;
        this.timeUnit = timeUnit;
    }

    public void injectToAll(ObsEvent obsEvent) {
        logger.trace("Inject: " + obsEvent.toString());
        obsStream.put(obsEvent);
    }

    public void injectByKey(ObsEvent obsEvent) {
        logger.trace("Inject: " + obsEvent.toString());
        assignmentStream.put(obsEvent);
    }

    @Override
    protected void onStart() {

    }

    /* init() visibility is protected. Normally the Server class only should call init. */
    public void initApp() {
        init();
    }

    @Override
    protected void onInit() {

        metricsPE = createPE(MetricsPE.class);

        Stream<ResultEvent> resultStream = createStream("Result Stream", new ResultKeyFinder(), metricsPE);

        modelPE = createPE(ModelPE.class);
        modelPE.setModel(model);
        modelPE.setNumVectors(numVectors);

        assignmentStream = createStream("Assignment Stream", new ClassIDKeyFinder(), modelPE);

        MaximizerPE maximizerPE = createPE(MaximizerPE.class);
        maximizerPE.setNumClasses(numClasses);
        maximizerPE.setAssignmentStream(assignmentStream);

        Stream<ObsEvent> distanceStream = createStream("Distance Stream", new ObsIndexKeyFinder(), maximizerPE);

        /*
         * There is a loop in this graph so we need to set the stream at the end. Is there a cleaner way to do this?
         */
        modelPE.setStream(distanceStream, resultStream);
        // modelPE.setOutputIntervalInEvents(10); // output every 10 events
        metricsPE.setTimerInterval(outputInterval, timeUnit); // output every 5
                                                               // seconds
        // obsStream = new Stream<ObsEvent>(this, "Observation Stream", new
        // ClassIDKeyFinder(), modelPE);
        obsStream = createStream("Observation Stream", modelPE);
    }

    /** @return true if modelPE is initialized. */
    public boolean isInited() {
        if (modelPE.getRemoteInstances().size() == numClasses)
            return true;
        else
            return false;
    }

    /**
     * Check if a training iteration is completed.
     * 
     * @param iteration
     * @return true when a training iteration is completed.
     */
    public boolean isTrained(int iteration) {

        int sum = 0;
        for (ProcessingElement pe : modelPE.getRemoteInstances().values()) {
            sum += ((ModelPE) pe).getIteration();
        }
        if (sum < iteration * numClasses)
            return false;
        else
            return true;
    }

    /**
     * Check if a test is complete.
     * 
     * @param number
     *            of test vectors.
     * @return true when testing is complete.
     */
    public boolean isTested(long numTestVectors) {

        long count = 0;
        MetricsPE mpe = null;
        for (ProcessingElement pe : metricsPE.getRemoteInstances().values()) {
            mpe = (MetricsPE) pe;
            count = mpe.getCount();
        }
        if (count < numTestVectors) {
            return false;
        } else {
            report = mpe.toString();
            return true;
        }
    }

    /**
     * @return a metrics report.
     */
    public String getReport() {

        return report;
    }

    /* close() visibility is protected. Normally the Server class only should call init. */
    public void closeApp() {
        close();
    }

    @Override
    protected void close() {
    }

    public long getObsCount() {

        return modelPE.getObsCount();
    }

    @Override
    protected void onClose() {
    }
}
