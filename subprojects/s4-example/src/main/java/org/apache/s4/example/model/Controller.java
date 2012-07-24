/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.example.model;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.s4.core.Receiver;
import org.apache.s4.core.Sender;
import org.apache.s4.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Train a classifier, run a test, compute the accuracy of the classifier.
 */
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    final private String trainFilename;
    final private String testFilename;
    final private long numTrainVectors;
    final private long numTestVectors;
    final private int numIterations;
    final private int outputInterval;
    final private Model model;
    final private int vectorSize;
    private int numClasses;
    final private Sender sender;
    final private Receiver receiver;

    @Inject
    private Controller(@Named("model.train_data") String trainFilename, @Named("model.test_data") String testFilename,
            Model model, @Named("model.vector_size") int vectorSize, @Named("model.num_iterations") int numIterations,
            @Named("model.output_interval_in_seconds") int outputInterval,
            @Named("model.logger.level") String logLevel, Sender sender, Receiver receiver) {

        this.trainFilename = trainFilename;
        this.testFilename = testFilename;
        this.numTrainVectors = getNumLines(trainFilename);
        this.numTestVectors = getNumLines(testFilename);
        this.numIterations = numIterations;
        this.vectorSize = vectorSize;
        this.outputInterval = outputInterval;
        this.model = model;
        this.sender = sender;
        this.receiver = receiver;

        logger.info("Number of test vectors is " + numTestVectors);
        logger.info("Number of train vectors is " + numTrainVectors);
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));
    }

    public void start() {

        logger.info("Processing file: " + trainFilename);
        try {

            /* Get vector size and number of classes from data set. */
            getDataSetInfo(trainFilename);

            MyApp app = new MyApp(numClasses, numTrainVectors, model, outputInterval, TimeUnit.SECONDS);

            // app.setCommLayer(sender, receiver);

            logger.info("Init app.");
            app.initApp();

            /* Initialize modelPEs by injecting one dummy events per class. */
            for (int i = 0; i < numClasses; i++) {
                ObsEvent obsEvent = new ObsEvent(-1, new float[vectorSize], -Float.MAX_VALUE, i, -1, true);
                app.injectByKey(obsEvent);
            }

            /* Wait until the app is initialized. */
            while (!app.isInited()) {
                Thread.sleep(1);
            }

            long start = System.nanoTime();
            for (int i = 0; i < numIterations; i++) {
                logger.info("Starting iteration {}.", i);
                injectData(app, true, trainFilename);

                /*
                 * Make sure all the data has been processed.
                 */
                while (!app.isTrained(i)) {
                    Thread.sleep(5);
                }
            }
            long stop = System.nanoTime();
            long trainTime = stop - start;

            /* Start testing. */
            logger.info("Start testing.");
            start = System.nanoTime();
            injectData(app, false, testFilename);
            stop = System.nanoTime();
            long testTime = stop - start;

            while (!app.isTested(numTestVectors)) {
                Thread.sleep(5);
            }

            /* Print final report. */
            logger.info(app.getReport());

            /* Print timing info. */
            logger.info("Total training time was {} seconds.", trainTime / 1000000000);
            logger.info("Training time per observation was {} microseconds.", trainTime / numTrainVectors / 1000);
            logger.info("Training time per observation per iteration was {} microseconds.", trainTime / numTrainVectors
                    / numIterations / 1000);
            logger.info("Total testing time was {} seconds.", testTime / 1000000000);
            logger.info("Testing time per observation was {} microseconds.", testTime / numTrainVectors / 1000);

            /* Done. */
            app.closeApp();

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

    private void injectData(MyApp app, boolean isTraining, String filename) throws FileNotFoundException, IOException {

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
            ObsEvent obsEvent = new ObsEvent(count++, vector, -Float.MAX_VALUE, classID, -1, isTraining);
            app.injectToAll(obsEvent);
        }
        data.close();
    }

    private void getDataSetInfo(String filename) throws FileNotFoundException, IOException {

        Map<Integer, Long> countsPerClass = new HashMap<Integer, Long>();

        DataFile data = new DataFile(filename);

        for (String line : data) {

            String[] result = line.split("\\s");

            /* Format is: label val1 val2 ... valN */
            if (vectorSize != result.length - 1) {
                throw new IllegalArgumentException("vectorSize: (" + vectorSize
                        + ") does not match number of columns in data file (" + (result.length - 1) + ").");
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
