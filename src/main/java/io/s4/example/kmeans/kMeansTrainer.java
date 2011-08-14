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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.App;
import io.s4.Stream;

public class kMeansTrainer extends App {

	Logger logger = LoggerFactory.getLogger(kMeansTrainer.class);

	private int numClusters;
	private int vectorSize;
	private long numVectors;
	private float[][] initialCentroids;
	private Stream<ObsEvent> obsStream;

	public kMeansTrainer(int numClusters, int vectorSize, long numVectors,
			float[][] initialCentroids) {
		super();
		this.numClusters = numClusters;
		this.vectorSize = vectorSize;
		this.numVectors = numVectors;
		this.initialCentroids = initialCentroids;
	}

	public void injectData(int index, float[] obs) {	
		ObsEvent obsEvent = new ObsEvent(index, obs, -1.0f, -1);
		logger.trace("Inject: " + obsEvent.toString());
			obsStream.put(obsEvent);
	}
	
	@Override
	protected void start() {

	}

	@Override
	protected void init() {

		ClusterPE clusterPE = new ClusterPE(this, numClusters, vectorSize,
				numVectors, initialCentroids);

		Stream<ObsEvent> assignmentStream = new Stream<ObsEvent>(this,
				"Assignment Stream", new ClusterIDKeyFinder(), clusterPE);

		MinimizerPE minimizerPE = new MinimizerPE(this, numClusters,
				assignmentStream);

		Stream<ObsEvent> distanceStream = new Stream<ObsEvent>(this,
				"Distance Stream", new ObsIndexKeyFinder(), minimizerPE);

		/*
		 * There is a loop in this graph so we need to set the stream at the
		 * end. Is there a cleaner way to do this?
		 */
		clusterPE.setStream(distanceStream);

		/* This stream will send events of type ObsEvent to ALL the PE 
		 * instances in clusterPE. 
		 * */
		obsStream = new Stream<ObsEvent>(this, "Observation Stream",
				clusterPE);
	}

	@Override
	protected void close() {
		// TODO Auto-generated method stub

	}
}
