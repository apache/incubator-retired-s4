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
import io.s4.core.Stream;

public class MyApp extends App {

    private static final Logger logger = LoggerFactory.getLogger(MyApp.class);

    private int numClasses;
    private int vectorSize;
    private long numVectors;
    private Stream<ObsEvent> obsStream;

    private ModelPE modelPE;
    Stream<ObsEvent> assignmentStream;

    public MyApp(int numClasses, int vectorSize, long numVectors) {
        super();
        this.numClasses = numClasses;
        this.vectorSize = vectorSize;
        this.numVectors = numVectors;
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
    protected void start() {

    }

    @Override
    protected void init() {

        modelPE = new ModelPE(this, vectorSize, numVectors, numClasses);

        assignmentStream = new Stream<ObsEvent>(this, "Assignment Stream",
                new ClassIDKeyFinder(), modelPE);

        MinimizerPE minimizerPE = new MinimizerPE(this, numClasses,
                assignmentStream);

        Stream<ObsEvent> distanceStream = new Stream<ObsEvent>(this,
                "Distance Stream", new ObsIndexKeyFinder(), minimizerPE);

        /*
         * There is a loop in this graph so we need to set the stream at the
         * end. Is there a cleaner way to do this?
         */
        modelPE.setStream(distanceStream);
        modelPE.setOutputIntervalInEvents(10);
        // obsStream = new Stream<ObsEvent>(this, "Observation Stream", new
        // ClassIDKeyFinder(), modelPE);
        obsStream = new Stream<ObsEvent>(this, "Observation Stream", modelPE);
    }

    @Override
    protected void close() {
        // TODO Auto-generated method stub

    }

    public long getObsCount() {

        return modelPE.getObsCount();
    }

    public void remove() {
        removeAll();
    }
}
