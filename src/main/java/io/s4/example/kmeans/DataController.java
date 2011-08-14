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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataController {

	final private String TRAIN_FILENAME = "/covtype-train-1000.data.gz";
	final private String TEST_FILENAME = "/covtype-test.data.gz";
	final private int MAX_NUM_CLASSES = 10;
	final private long numTrainVectors;
	final private long numTestVectors;
	private int vectorSize;
	private int numClasses;
	private float[][] initialCentroids;

	Logger logger = LoggerFactory.getLogger(DataController.class);

	public DataController() {

		this.numTrainVectors = getNumLines(TRAIN_FILENAME);
		this.numTestVectors = getNumLines(TEST_FILENAME);

		logger.info("Number of test vectors is " + numTestVectors);
		logger.info("Number of train vectors is " + numTrainVectors);
	}

	public void start() {

		logger.info("Processing file: " + TRAIN_FILENAME);
		try {
			getRandomVectors(TRAIN_FILENAME);

			kMeansTrainer app = new kMeansTrainer(numClasses, vectorSize,
					numTrainVectors, initialCentroids);

			logger.info("Init app.");
			app.init();
			
			injectData(app, TRAIN_FILENAME);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void injectData(kMeansTrainer app, String filename)
			throws FileNotFoundException, IOException {

		DataFile data = new DataFile(filename);
		int count = 0;
		for (String line : data) {

			String[] result = line.split("\\s");

			float[] vector = new float[vectorSize];
			for (int j = 0; j < vectorSize; j++) {

				vector[j] = Float.parseFloat(result[j + 1]);
			}
			app.injectData(count++, vector);
		}
		data.close();
	}

	private long getRandomVectors(String filename)
			throws FileNotFoundException, IOException {

		/*
		 * We assume that the data file is randomized. All we need to do is pick
		 * the first vector of each class.
		 */

		// float[][] vectors =
		Map<Integer, Long> countsPerClass = new HashMap<Integer, Long>();
		long totalCount = 0;

		DataFile data = new DataFile(filename);

		for (String line : data) {

			totalCount++;
			String[] result = line.split("\\s");

			/* Format is: label val1 val2 ... valN */
			if (vectorSize == 0) {
				vectorSize = result.length - 1;
				initialCentroids = new float[MAX_NUM_CLASSES][vectorSize];
			}

			/* Class ID range starts in 1, shift to start in zero. */
			int classID = Integer.parseInt(result[0]) - 1;

			/* WHen we see a class for the first time do. */
			if (!countsPerClass.containsKey(classID)) {

				for (int j = 0; j < vectorSize; j++) {
					initialCentroids[classID][j] = Float
							.parseFloat(result[j + 1]);
				}
				countsPerClass.put(classID, 1L);
			} else {
				long count = countsPerClass.get(classID) + 1;
				countsPerClass.put(classID, count);
			}
		}
		data.close();

		/* Summary. */
		numClasses = countsPerClass.size();
		logger.info("Number of classes is " + numClasses);

		/* Remove all the instances. */
		for (Map.Entry<Integer, Long> entry : countsPerClass.entrySet()) {

			int key = entry.getKey();
			long val = entry.getValue();

			logger.info("Num vectors for class ID: " + key + " is " + val);
		}

		return totalCount;
	}

	private long getNumLines(String filename) {

		long count = 0;
		try {
			DataFile data = new DataFile(filename);

			// System.out.println("NUM LINES: " + trainData.getNumLines());
			for (@SuppressWarnings("unused")
			String line : data) {
				count++;
			}
			data.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return count;
	}
}
