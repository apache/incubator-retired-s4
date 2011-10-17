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

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s4.core.App;
import io.s4.base.Event;
import io.s4.core.ProcessingElement;

final public class MetricsPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory
            .getLogger(MetricsPE.class);

    private Map<Integer, HashMap<Integer, MutableInt>> counts;
    private long totalCount = 0;
    private int numClasses;

    public MetricsPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {

        ResultEvent resultEvent = (ResultEvent) event;
        int classID = resultEvent.getClassId();
        int hypID = resultEvent.getHypId();
        totalCount += 1;

        /* Increment counter. */
        if (!counts.containsKey(classID)) {
            counts.put(classID, new HashMap<Integer, MutableInt>());
            numClasses++;
        }
        MutableInt value = counts.get(classID).get(hypID);
        if (value == null) {
            value = new MutableInt();
            counts.get(classID).put(hypID, value);
        }
        value.inc();
    }

    public void onTrigger(Event event) {
        logger.info(this.toString());
    }

    @Override
    protected void onCreate() {
        counts = new HashMap<Integer, HashMap<Integer, MutableInt>>();
    }

    @Override
    protected void onRemove() {
    }

    /** @return number of data vectors processed. */
    public long getCount() {
        return totalCount;
    }

    @Override
    public String toString() {
        StringBuilder report = new StringBuilder();
        report.append("\n\nConfusion Matrix [%]:\n");
        report.append("\n      ");
        for (int i = 0; i < numClasses; i++) {
            report.append(String.format("%6d", i));
        }
        report.append("\n        ----------------------------------------\n");
        long truePositives = 0;
        for (Map.Entry<Integer, HashMap<Integer, MutableInt>> entry : counts
                .entrySet()) {

            int classID = entry.getKey();
            report.append(String.format("%5d:", classID));
            HashMap<Integer, MutableInt> hypCounts = entry.getValue();

            long totalCountForClass = getTotalCountForClass(hypCounts);
            float[] sortedCounts = new float[numClasses];
            for (Map.Entry<Integer, MutableInt> hypEntry : hypCounts.entrySet()) {
                int hypID = hypEntry.getKey();
                long count = hypEntry.getValue().get();

                /*
                 * Because of timing, it is possible to have a hypId that was
                 * not counted in numClasses yet. In this case we bail out and
                 * without producing a report.
                 */
                if (hypID > (numClasses - 1))
                    return "Insufficient data.";

                sortedCounts[hypID] = (float) count / totalCountForClass * 100f;
                if (classID == hypID)
                    truePositives += count;
            }
            for (int i = 0; i < numClasses; i++) {
                report.append(String.format("%6.1f", sortedCounts[i]));
            }

            report.append("\n");
        }
        report.append(String.format(
                "\nAccuracy: %6.1f%% - Num Observations: %6d\n",
                (float) truePositives / totalCount * 100f, totalCount));

        return report.toString();
    }

    private long getTotalCountForClass(HashMap<Integer, MutableInt> counts) {

        long count = 0;
        for (Map.Entry<Integer, MutableInt> hypEntry : counts.entrySet()) {
            count += hypEntry.getValue().get();
        }
        return count;
    }

    private class MutableInt {
        private int value = 0;

        private void inc() {
            ++value;
        }

        private int get() {
            return value;
        }
    }
}
