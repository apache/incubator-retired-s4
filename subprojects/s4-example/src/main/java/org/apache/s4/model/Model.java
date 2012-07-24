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

/**
 * 
 * Base class for statistical models.
 * 
 * 
 */
abstract public class Model {

    protected boolean isTrain;
    protected String name;

    public Model() {
    }

    public Model(String name, boolean isTrain) {
        this.name = name;
        this.isTrain = isTrain;
    }

    /**
     * Return an instance of this model initialized with the same parameters as its parent.
     * 
     * @return a copy of the parent model.
     */
    abstract public Model create();

    /**
     * Compute the probability of the observed vector.
     * 
     * @param obs
     *            An observed data vector.
     * @return the probability.
     */
    abstract public double prob(float[] obs);

    /**
     * Compute the log probability of the observed vector.
     * 
     * @param obs
     *            An observed data vector.
     * @return the log probability.
     */
    abstract public double logProb(float[] obs);

    /**
     * Update sufficient statistics/
     * 
     * @param obs
     *            An observed data vector.
     */
    abstract public void update(float[] obs);

    /** Estimate model parameters. */
    abstract public void estimate();

    /** Clear sufficient statistics. */
    abstract public void clearStatistics();

    /** @return true if training resources have been initialized. */
    public boolean isTrain() {
        return isTrain;
    }

    public void setTrain(boolean isTrain) {
        this.isTrain = isTrain;
    }

    /** @return model name. */
    public String getName() {
        return name;
    }

    /** Set model name. */
    public void setName(String name) {
        this.name = name;
    }
}
