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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.comm.Emitter;
import io.s4.comm.QueueingEmitter;
import io.s4.comm.QueueingListener;
import io.s4.comm.Receiver;
import io.s4.comm.Sender;
import io.s4.comm.loopback.LoopBackEmitter;
import io.s4.comm.loopback.LoopBackListener;
import io.s4.comm.netty.NettyEmitter;
import io.s4.comm.netty.NettyListener;
import io.s4.comm.topology.Assignment;
import io.s4.comm.topology.AssignmentFromFile;
import io.s4.comm.topology.Topology;
import io.s4.comm.topology.TopologyFromFile;
import io.s4.core.App;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;
import io.s4.model.Model;
import io.s4.serialize.KryoSerDeser;
import io.s4.serialize.SerializerDeserializer;

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

    MyApp(int numClasses, long numVectors, Model model, int outputInterval,
            TimeUnit timeUnit) {
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
        System.out.println("Hey!!");
        logger.trace("Inject: " + obsEvent.toString());
        assignmentStream.put(obsEvent);
    }

    @Override
    protected void start() {

    }

    @Override
    protected void init() {
        // TODO: probably the wrong place to create commlayer stuff
        // TODO: probably the wrong place to create commlayer stuff
        String clusterName = "s4";
        String configFilename = "clusters.xml";
        
        Assignment assignment = new AssignmentFromFile(clusterName, configFilename);
        Topology topology = new TopologyFromFile(clusterName, configFilename);
        
        NettyListener listener = new NettyListener(assignment);
        NettyEmitter emitter = new NettyEmitter(topology);
        
        //UDPListener llListener = new UDPListener(assignment, 0);
        //UDPEmitter llEmitter = new UDPEmitter(topology);
        
        //LoopBackListener llListener = new LoopBackListener();
        //Emitter llEmitter = new LoopBackEmitter(lBlistener);
        
        //QueueingEmitter emitter = new QueueingEmitter(llEmitter, 8000);
        //emitter.start();
        //QueueingListener listener = new QueueingListener(llListener, 8000);
        //listener.start();
        
        SerializerDeserializer serDeser = new KryoSerDeser();
        
        Sender sender = new Sender(emitter, serDeser);
        Receiver receiver = new Receiver(listener, serDeser);

        metricsPE = new MetricsPE(this);

        Stream<ResultEvent> resultStream = new Stream<ResultEvent>(this,
                "Result Stream", new ResultKeyFinder(), sender, receiver, metricsPE);

        modelPE = new ModelPE(this, model, numVectors);

        assignmentStream = new Stream<ObsEvent>(this, "Assignment Stream",
                new ClassIDKeyFinder(), sender, receiver, modelPE);

        MaximizerPE minimizerPE = new MaximizerPE(this, numClasses,
                assignmentStream);

        Stream<ObsEvent> distanceStream = new Stream<ObsEvent>(this,
                "Distance Stream", new ObsIndexKeyFinder(), sender, receiver, minimizerPE);

        /*
         * There is a loop in this graph so we need to set the stream at the
         * end. Is there a cleaner way to do this?
         */
        modelPE.setStream(distanceStream, resultStream);
        // modelPE.setOutputIntervalInEvents(10); // output every 10 events
        metricsPE.setOutputInterval(outputInterval, timeUnit, true); // output every 5
                                                               // seconds
        // obsStream = new Stream<ObsEvent>(this, "Observation Stream", new
        // ClassIDKeyFinder(), modelPE);
        obsStream = new Stream<ObsEvent>(this, "Observation Stream", sender, receiver, modelPE);
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
