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

package org.apache.s4.model;

import java.util.Random;

import org.ejml.data.DenseMatrix64F;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestGaussianModel extends TestCase {

    private int NUM_VECTORS = 100000;
    private double mean[] = { 153, 10.0, 5.0, 0.1 };
    private double std[] = { 30, 2.0, 1.0, 5.5 };
    private int numElements = mean.length;
    private DenseMatrix64F vectors[] = new DenseMatrix64F[NUM_VECTORS];
    private double doubleArrays[][] = new double[NUM_VECTORS][numElements];
    private float floatArrays[][] = new float[NUM_VECTORS][numElements];

    private Random random = new Random(0);

    protected void setUp() {

        /* Generate the data set. */
        for (int i = 0; i < NUM_VECTORS; i++) {
            vectors[i] = new DenseMatrix64F(numElements, 1);
            for (int j = 0; j < numElements; j++) {
                double v = mean[j] + std[j] * random.nextGaussian();
                vectors[i].set(j, v);
                doubleArrays[i][j] = v;
                floatArrays[i][j] = (float)v;
            }
        }
    }

    public void testTrainerUsingEJML() {
        GaussianModel gm = new GaussianModel(numElements, true);
        for (int i = 0; i < NUM_VECTORS; i++) {
            gm.update(vectors[i]);
        }
        gm.estimate();
        System.out.println(gm);

        double[] actualMean = gm.getMean();

        for (int j = 0; j < mean.length; j++) {

            Assert.assertEquals("Assert mean.", mean[j], actualMean[j], std[j]);
        }
    }

    public void testTrainerUsingDoubleArray() {
        GaussianModel gm = new GaussianModel(numElements, true);
        for (int i = 0; i < NUM_VECTORS; i++) {
            gm.update(doubleArrays[i]);
        }
        gm.estimate();
        System.out.println(gm);

        double[] actualMean = gm.getMean();

        for (int j = 0; j < mean.length; j++) {

            Assert.assertEquals("Assert mean.", mean[j], actualMean[j], std[j]);
        }
    }

    public void testTrainerUsingFloatArray() {
        GaussianModel gm = new GaussianModel(numElements, true);
        for (int i = 0; i < NUM_VECTORS; i++) {
            gm.update(floatArrays[i]);
        }
        gm.estimate();
        System.out.println(gm);

        double[] actualMean = gm.getMean();

        for (int j = 0; j < mean.length; j++) {

            Assert.assertEquals("Assert mean.", mean[j], actualMean[j], std[j]);
        }
    }
}
