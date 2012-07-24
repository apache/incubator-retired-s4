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

import org.apache.commons.lang.NotImplementedException;
import org.apache.s4.util.MatrixOps;
import org.ejml.data.D1Matrix64F;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;


/**
 * A multivariate Gaussian mixture model. Only diagonal covariance matrices are
 * supported.
 * 
 * 
 */
public class GaussianMixtureModel extends Model {

    /** Supported algorithms for training this model. */
    public enum TrainMethod {

        /**
         * Estimate mean and variance of Gaussian distribution in the first
         * iteration. Create the target number of Gaussian components
         * (numComponents) in the mixture at the end of the first iteration
         * using the estimated mean and variance.
         */
        STEP,

        /**
         * Double the number of Gaussian components at the end of each
         * iteration.
         */
        DOUBLE,

        /** Do not allocate structures for training. */
        NONE
    }

    final private int numElements;
    final private TrainMethod trainMethod;
    private int numComponents;
    private double numSamples;
    private D1Matrix64F posteriorSum;
    private D1Matrix64F weights;
    private D1Matrix64F logWeights;
    private D1Matrix64F tmpProbs1;
    private D1Matrix64F tmpProbs2;
    private double totalLikelihood;
    private GaussianModel[] components;
    private int iteration = 0;

    public GaussianMixtureModel(int numElements, int numComponents,
            TrainMethod trainMethod) {
        super();
        this.numComponents = numComponents;
        this.numElements = numElements;
        this.trainMethod = trainMethod;

        if (trainMethod == TrainMethod.DOUBLE)
            throw new NotImplementedException(
                    "Want this? Join as a contributor at http://s4.io");

        /* Allocate arrays needed for estimation. */
        isTrain = false;
        if (trainMethod != TrainMethod.NONE) {
            setTrain(true);

            if (trainMethod == TrainMethod.STEP) {

                /* Set up for first iteration using a single Gaussian. */
                allocateTrainDataStructures(1);
            }
        }
    }

    /*
     * TODO: we use this method when the pattern is: create a prototype of the
     * model and use it to create instances. Notice that we are allocating data
     * structures in the prototype that we will not use in this case. Not a big
     * deal but we may want to optimize this somehow later.
     */
    public Model create() {

        return new GaussianMixtureModel(numElements, numComponents, trainMethod);
    }

    private void allocateTrainDataStructures(int numComp) {

        components = new GaussianModel[numComp];
        for (int i = 0; i < numComp; i++) {
            this.components[i] = new GaussianModel(numElements, true);

            this.weights = new DenseMatrix64F(numComp, 1);
            CommonOps.set(this.weights, 1.0 / numComp);
            this.logWeights = new DenseMatrix64F(numComp, 1);
            CommonOps.set(this.logWeights, Math.log(1.0 / numComp));
            posteriorSum = new DenseMatrix64F(numComp, 1);
            tmpProbs1 = new DenseMatrix64F(numComp, 1);
            tmpProbs2 = new DenseMatrix64F(numComp, 1);
        }
    }

    /*
     * This method is used in {@link TrainMethod#STEP} Convert to a mixture with
     * N components. This method guarantees that the data structures are created
     * and that all the variables are set for starting a new training iteration.
     */
    private void increaseNumComponents(int newNumComponents) {

        /*
         * We use the Gaussian distribution of the parent GMM to create the
         * children.
         */

        /* Get mean and variance from parent before we allocate resized data structures. */
        D1Matrix64F mean = MatrixOps.doubleArrayToMatrix(this.components[0]
                .getMean());
        D1Matrix64F variance = MatrixOps.doubleArrayToMatrix(this.components[0]
                .getVariance());

        /* Throw away all previous data structures. */
        allocateTrainDataStructures(newNumComponents);

        /*
         * Create new mixture components. Abandon the old ones. We already got
         * the mean and variance in the previous step.
         */
        for (int i = 0; i < newNumComponents; i++) {
            components[i].setMean(MatrixOps.createRandom(i, mean, variance));
            components[i].setVariance(new DenseMatrix64F(variance));
        }
    }

    /* Thread safe internal logProb method. Must pass temp array. */
    private double logProbInternal(D1Matrix64F obs, D1Matrix64F probs) {

        /* Compute log probabilities for this observation. */
        for (int i = 0; i < components.length; i++) {
            probs.set(i, components[i].logProb(obs) + logWeights.get(i));
        }

        /*
         * To simplify computation, use the max prob in the denominator instead
         * of the sum.
         */
        return CommonOps.elementMax(probs);
    }

    public double logProb(D1Matrix64F obs) {

        return logProbInternal(obs, tmpProbs1);
    }

    public double logProb(double[] obs) {

        return logProb(MatrixOps.doubleArrayToMatrix(obs));
    }

    public double logProb(float[] obs) {

        return logProb(MatrixOps.floatArrayToMatrix(obs));
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the probability.
     */
    public double prob(D1Matrix64F obs) {

        return Math.exp(logProb(obs));
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the probability.
     */
    public double prob(double[] obs) {

        return prob(MatrixOps.doubleArrayToMatrix(obs));
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the probability.
     */
    public double prob(float[] obs) {

        return prob(MatrixOps.floatArrayToMatrix(obs));

    }

    /** Update using Matrix array. */
    public void update(D1Matrix64F obs) {

        if (isTrain() == true) {

            /* Compute log probabilities for this observation. */
            double maxProb = logProbInternal(obs, tmpProbs2);
            totalLikelihood += maxProb;
            CommonOps.add(tmpProbs2, -maxProb);

            /* Compute posterior probabilities. */
            MatrixOps.elementExp(tmpProbs2);

            /* Update posterior sum, needed to compute mixture weights. */
            CommonOps.addEquals(posteriorSum, tmpProbs2);

            for (int i = 0; i < components.length; i++) {
                components[i].update(obs, tmpProbs2.get(i));
            }

            /* Count number of observations. */
            numSamples++;
        }
    }

    public void update(double[] obs) {
        update(MatrixOps.doubleArrayToMatrix(obs));
    }

    /** Update using float array. */
    public void update(float[] obs) {
        update(MatrixOps.floatArrayToMatrix(obs));
    }

    @Override
    public void estimate() {

        if (isTrain() == true) {

            /* Estimate mixture weights. */
            // double sum = CommonOps.elementSum(posteriorSum);
            // CommonOps.scale(1.0/sum, posteriorSum, weights);
            CommonOps.scale(1.0 / numSamples, posteriorSum, weights);
            MatrixOps.elementLog(weights, logWeights);

            /* Estimate component density. */
            for (int i = 0; i < components.length; i++) {
                components[i].estimate();
            }

            /*
             * After the first iteration, we can estimate the target number of
             * mixture components.
             */
            if (iteration == 0 && trainMethod == TrainMethod.STEP) {

                increaseNumComponents(numComponents);
            }
            iteration++;
        }
    }

    @Override
    public void clearStatistics() {
        if (isTrain() == true) {

            for (int i = 0; i < components.length; i++) {
                components[i].clearStatistics();
            }
            CommonOps.set(posteriorSum, 0.0);
            numSamples = 0;
            totalLikelihood = 0;
        }

    }

    /** Number of Gaussian components in the mixture. */
    public int getNumComponents() {
        return this.numComponents;
    }

    /** Data vector size. */
    public int getNumElements() {
        return this.numElements;
    }

    /**
     * @return the value of the parameters and sufficient statistics of this
     *         model in a printable format.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder("");
        sb.append("Gaussian Mixture Model\n");
        sb.append("num samples: " + numSamples + "\n");
        sb.append("num components: " + components.length + "\n");
        sb.append("weights: " + weights.toString() + "\n");
        sb.append("log weights: " + logWeights.toString() + "\n");
        sb.append("total log likelihood: " + totalLikelihood + "\n");

        for (int i = 0; i < components.length; i++) {
            sb.append(components[i].toString());
        }

        return sb.toString();
    }

}
