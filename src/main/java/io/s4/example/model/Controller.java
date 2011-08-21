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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Train a classifier, run a test, compute the accuracy of the classifier.
 */
public class Controller {

    // final private String TRAIN_FILENAME = "/covtype-train-1000.data.gz"; //
    // small file for debugging.
    final private String TRAIN_FILENAME = "/covtype-train.data.gz";
    final private String TEST_FILENAME = "/covtype-test.data.gz";
    final private long numTrainVectors;
    final private long numTestVectors;
    private int vectorSize;
    private int numClasses;

    Logger logger = LoggerFactory.getLogger(Controller.class);

    public Controller() {

        this.numTrainVectors = getNumLines(TRAIN_FILENAME);
        this.numTestVectors = getNumLines(TEST_FILENAME);

        logger.info("Number of test vectors is " + numTestVectors);
        logger.info("Number of train vectors is " + numTrainVectors);
    }

    public void start() {

        logger.info("Processing file: " + TRAIN_FILENAME);
        try {

            /* Get vector size and number of classes from data set. */
            getDataSetInfo(TRAIN_FILENAME);

            MyApp app = new MyApp(numClasses, vectorSize, numTrainVectors);

            logger.info("Init app.");
            app.init();

            /* For now we only need one iteration. */
            for (int i = 0; i < 1; i++) {
                logger.info("Starting iteration {}.", i);
                injectData(app, true, TRAIN_FILENAME);

                /*
                 * Make sure all the data has been processed. ModelPE will reset
                 * the total count after all the data is processed so we wait
                 * until the count is equal to zero. TODO
                 */
                Thread.sleep(10000);
                logger.info("End of iteration {}.", i);
            }

            /* Start testing. */
            logger.info("Start testing.");
            injectData(app, false, TEST_FILENAME);

            /* Done. */
            app.remove();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    private void injectData(MyApp app, boolean isTraining, String filename)
            throws FileNotFoundException, IOException {

        DataFile data = new DataFile(filename);
        int count = 0;
        for (String line : data) {

            String[] result = line.split("\\s");

            /* Class ID range starts in 1, shift to start in zero. */
            int classID = Integer.parseInt(result[0]) - 1;

            float[] vector = new float[vectorSize];
            for (int j = 0; j < vectorSize; j++) {

                vector[j] = Float.parseFloat(result[j + 1]);
            }
            ObsEvent obsEvent = new ObsEvent(count++, vector, -1.0f, classID,
                    -1, isTraining);
            app.injectData(obsEvent);
        }
        data.close();
    }

    private void getDataSetInfo(String filename) throws FileNotFoundException,
            IOException {

        Map<Integer, Long> countsPerClass = new HashMap<Integer, Long>();

        DataFile data = new DataFile(filename);

        for (String line : data) {

            String[] result = line.split("\\s");

            /* Format is: label val1 val2 ... valN */
            if (vectorSize == 0) {
                vectorSize = result.length - 1;
            }

            /* Class ID range starts in 1, shift to start in zero. */
            int classID = Integer.parseInt(result[0]) - 1;

            /* Count num vectors per class. */
            if (!countsPerClass.containsKey(classID)) {
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
        logger.info("Vector size is " + vectorSize);

        for (Map.Entry<Integer, Long> entry : countsPerClass.entrySet()) {

            int key = entry.getKey();
            long val = entry.getValue();

            logger.info("Num vectors for class ID: " + key + " is " + val);
        }
    }

    /*
     * @return Returns the number of lines in a text file.
     */
    private long getNumLines(String filename) {

        long count = 0;
        try {
            DataFile data = new DataFile(filename);

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
