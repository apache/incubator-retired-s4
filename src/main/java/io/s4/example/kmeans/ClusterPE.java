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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.App;
import io.s4.Event;
import io.s4.ProcessingElement;
import io.s4.Stream;

public class ClusterPE extends ProcessingElement {

	Logger logger = LoggerFactory.getLogger(ClusterPE.class);

	final private int numClusters;
	final private int vectorSize;
	final private long numVectors;
	private Stream<ObsEvent> distanceStream;
	private int clusterId;
	private float[] centroid;
	private long obsCount = 0;
	private float[] obsSum;
	private float totalDistance = 0f;
	private int[][] confusionMatrix;

	public ClusterPE(App app, int numClusters, int vectorSize, long numVectors,
			float[][] centroids) {
		super(app);
		this.numClusters = numClusters;
		this.vectorSize = vectorSize;
		this.numVectors = numVectors;

		confusionMatrix = new int[numClusters][numClusters];
		/*
		 * The ClusterPE instances are not event driven. That is they are
		 * created here before events start to flow.
		 * 
		 * The total number of PE instances is given by numClusters.
		 */
		for (int i = 0; i < numClusters; i++) {
			ClusterPE pe = (ClusterPE) this.getInstanceForKey(Integer
					.toString(i));
			pe.setClusterId(i);
			pe.setCentroid(centroids[i]);
		}
	}

	public void setStream(Stream<ObsEvent> distanceStream) {

		/* Init prototype. */
		this.distanceStream = distanceStream;

		/*
		 * We also need to set the stream in the instances we created in the
		 * constructor.
		 */
		List<ProcessingElement> pes = this.getAllInstances();

		/* STEP 2: iterate and pass event to PE instance. */
		for (ProcessingElement pe : pes) {

			((ClusterPE) pe).distanceStream = distanceStream;
		}
	}

	public void setClusterId(int clusterId) {
		this.clusterId = clusterId;
	}

	public void setCentroid(float[] centroid) {
		this.centroid = centroid;
	}

	public long getObsCount() {
		return obsCount;
	}

	synchronized private void updateTotalStats(ObsEvent event) {

		/* Update global stats in the prototype in the prototype. */
		ClusterPE clusterPEPrototype = (ClusterPE) pePrototype;
		clusterPEPrototype.obsCount++;
		clusterPEPrototype.totalDistance += event.getDistance();
		clusterPEPrototype.confusionMatrix[event.getClassId()][event.getHypId()] += 1;

		logger.trace("Index: " + event.getIndex() + ", Label: "
				+ event.getClassId() + ", Hyp: " + event.getHypId()
				+ ", Total Count: " + clusterPEPrototype.obsCount
				+ ", Total Dist: " + clusterPEPrototype.totalDistance);

		/* Log info. */
		if (clusterPEPrototype.obsCount % 10000 == 0) {
			logger.info("Processed {} events", clusterPEPrototype.obsCount);
			logger.info("Average distance is {}.",
					clusterPEPrototype.totalDistance
							/ clusterPEPrototype.obsCount);
		}

		if (clusterPEPrototype.obsCount == numVectors) {

			/* Done processing training set. */

			logger.info("Final Count: {}.", clusterPEPrototype.obsCount);
			logger.info("Final Average Distance: {}.",
					clusterPEPrototype.totalDistance
							/ clusterPEPrototype.obsCount);

			for (int i = 0; i < numClusters; i++)
				for (int j = 0; j < numClusters; j++) {
					
					Object[] paramArray = { i, j,
							clusterPEPrototype.confusionMatrix[i][j] };
					
					logger.info(
							"Final Count of class {} classified as {}: {}.",
							paramArray);
				}

			/* Update centroids. */
			for (Map.Entry<String, ProcessingElement> entry : peInstances
					.entrySet()) {

				ClusterPE pe = (ClusterPE) entry.getValue();
				// String key = entry.getKey();
				pe.updateCentroid();

			}

			/* Reset global stats. */
			clusterPEPrototype.obsCount = 0;
			clusterPEPrototype.totalDistance = 0f;
			clusterPEPrototype.confusionMatrix = new int[numClusters][numClusters];
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

	private void updateCentroid() {

		for (int i = 0; i < vectorSize; i++) {
			centroid[i] = obsSum[i] / obsCount;
			obsSum[i] = 0f;
		}

		obsCount = 0;
	}

	/*
	 * 
	 * @see io.s4.ProcessingElement#processInputEvent(io.s4.Event)
	 * 
	 * Read input event, compute distance to current centroid and emit.
	 */
	@Override
	protected void processInputEvent(Event event) {

		ObsEvent inEvent = (ObsEvent) event;
		float[] obs = inEvent.getObsVector();

		/* The raw ObsEvent should have the distance set to less than 0.0. */
		if (inEvent.getDistance() < 0f) {

			/* Process raw event. */
			float dist = distance(obs);
			ObsEvent outEvent = new ObsEvent(inEvent.getIndex(), obs, dist,
					inEvent.getClassId(), clusterId);
			logger.trace("IN: " + inEvent.toString());
			logger.trace("OUT: " + outEvent.toString());
			distanceStream.put(outEvent);

		} else {

			/* This is a labeled event. Update sufficient statistics. */
			logger.trace("LABELED IN: " + inEvent.toString());

			/* Update obs count for this class. */
			obsCount++;

			/* Log info. */
			if (obsCount % 1000 == 0) {
				logger.info("Labeled {} events with class id {}", obsCount,
						clusterId);
			}

			/* Update total obs count and distance. */
			updateTotalStats(inEvent);

			for (int i = 0; i < vectorSize; i++) {
				obsSum[i] += obs[i];
			}
		}
	}

	@Override
	public void sendEvent() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void initPEInstance() {

		/* Create an array for each PE instance. */
		this.obsSum = new float[vectorSize];

	}

	@Override
	protected void removeInstanceForKey(String id) {
		// TODO Auto-generated method stub

	}

}
