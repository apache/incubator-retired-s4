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
package org.apache.s4.model;


import org.apache.s4.util.MatrixOps;
import org.ejml.data.D1Matrix64F;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

/**
 * A multivariate Gaussian model with parameters mean (mu) and variance (sigma
 * squared). Only diagonal covariance matrices are supported.
 * 
 * @author Leo Neumeyer
 * 
 */
public class GaussianModel extends Model {

    public final static double SMALL_VARIANCE = 0.01f;
    public final static double minNumSamples = 0.01f;

    private boolean isDiagonal = true; // Full covariance not yet supported.
    private D1Matrix64F sumx;
    private D1Matrix64F sumxsq;
    private D1Matrix64F tmpArray;
    private double numSamples;
    private D1Matrix64F mean;
    private D1Matrix64F variance; // ==> sigma squared
    private int numElements;

    private double const1; // -(N/2)log(2PI) Depends only on numElements.
    private double const2; // const1 - sum(log sigma_i) Also depends on
                           // variance.

    /**
     * @param numElements
     *            the model dimension.
     * @param train
     *            allocate training arrays when true.
     */
    public GaussianModel(int numElements, boolean train) {
        this(numElements, null, null, train);
    }

    /**
     * Initialize model, no allocation of training arrays.
     * 
     * @param numElements
     *            the model dimension.
     * @param mean
     *            model parameter.
     * @param variance
     *            model parameter.
     */
    public GaussianModel(int numElements, D1Matrix64F mean, D1Matrix64F variance) {
        this(numElements, mean, variance, false);
    }

    /**
     * Initialize model, no allocation of training arrays.
     * 
     * @param numElements
     *            the model dimension.
     * @param mean
     *            model parameter.
     * @param variance
     *            model parameter.
     * @param train
     *            allocate training arrays when true.
     */
    public GaussianModel(int numElements, D1Matrix64F mean,
            D1Matrix64F variance, boolean train) {
        super();
        this.numElements = numElements;
        tmpArray = new DenseMatrix64F(numElements, 1);

        if (mean == null) {
            this.mean = new DenseMatrix64F(numElements, 1);
        } else {
            this.mean = mean;
        }

        if (variance == null) {
            this.variance = new DenseMatrix64F(numElements, 1);
            CommonOps.set(this.variance, SMALL_VARIANCE);
        } else {
            this.variance = variance;
        }

        const1 = -numElements * (float) Math.log(2 * Math.PI) / 2;
        MatrixOps.elementLog(this.variance, tmpArray);
        const2 = const1 - CommonOps.elementSum(tmpArray) / 2.0;

        /* Allocate arrays needed for estimation. */
        if (train == true) {
            setTrain(true);
            sumx = new DenseMatrix64F(numElements, 1);
            sumxsq = new DenseMatrix64F(numElements, 1);
            clearStatistics();
        } else {
            setTrain(false);
        }

    }

    public Model create() {

        return new GaussianModel(numElements, isTrain);
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the log probability.
     */
    public double logProb(D1Matrix64F obs) {

        CommonOps.sub(mean, obs, tmpArray);
        MatrixOps.elementSquare(tmpArray);
        CommonOps.elementDiv(tmpArray, variance);
        return const2 - CommonOps.elementSum(tmpArray) / 2.0;
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the log probability.
     */
    public double logProb(float[] obs) {

        return logProb(MatrixOps.floatArrayToMatrix(obs));
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the log probability.
     */
    public double logProb(double[] obs) {

        return logProb(MatrixOps.doubleArrayToMatrix(obs));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.model.Model#evaluate(double[])
     */
    public double prob(double[] obs) {

        return prob(MatrixOps.doubleArrayToMatrix(obs));
    }

    /** Evaluate using float array. */
    public double prob(float[] obs) {

        return prob(MatrixOps.floatArrayToMatrix(obs));
    }

    /**
     * @param obs
     *            the observed data vector.
     * @return the probability.
     */
    public double prob(D1Matrix64F obs) {

        return Math.exp(logProb(obs));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.model.Model#update(double[])
     */
    public void update(double[] obs) {

        update(MatrixOps.doubleArrayToMatrix(obs));

    }

    /** Update using float array. */
    public void update(float[] obs) {

        update(MatrixOps.floatArrayToMatrix(obs));

    }

    /**
     * Update sufficient statistics.
     * 
     * @param obs
     *            the observed data vector.
     */
    public void update(D1Matrix64F obs) {

        if (isTrain() == true) {

            /* Update sufficient statistics. */
            CommonOps.add(obs, sumx, sumx);
            MatrixOps.elementSquare(obs, tmpArray);
            CommonOps.add(tmpArray, sumxsq, sumxsq);
            numSamples++;
        }
    }

    /**
     * Update sufficient statistics.
     * 
     * @param obs
     *            the observed data vector.
     * @param weight
     *            the weight assigned to this observation.
     */
    public void update(D1Matrix64F obs, double weight) {

        if (isTrain() == true) {

            /* Update sufficient statistics. */
            CommonOps.scale(weight, obs, tmpArray);
            CommonOps.add(tmpArray, sumx, sumx);

            MatrixOps.elementSquare(obs, tmpArray);
            CommonOps.scale(weight, tmpArray);
            CommonOps.add(tmpArray, sumxsq, sumxsq);

            numSamples += weight;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.model.Model#estimate()
     */
    public void estimate() {

        if (numSamples > minNumSamples) {

            /* Estimate the mean. */
            CommonOps.scale(1.0 / numSamples, sumx, mean);

            /*
             * Estimate the variance. sigma_sq = 1/n (sumxsq - 1/n sumx^2) or
             * 1/n sumxsq - mean^2.
             */
            D1Matrix64F tmp = variance; // borrow as an intermediate array.

            MatrixOps.elementSquare(mean, tmpArray);
            CommonOps.scale(1.0 / numSamples, sumxsq, tmp);

            CommonOps.sub(tmp, tmpArray, variance);

            MatrixOps.elementFloor(SMALL_VARIANCE, variance, variance);

        } else {

            /* Not enough training sample. */
            CommonOps.set(variance, SMALL_VARIANCE);
            CommonOps.set(mean, 0.0);
        }

        /* Update log Gaussian constant. */
        MatrixOps.elementLog(this.variance, tmpArray);
        const2 = const1 - CommonOps.elementSum(tmpArray) / 2.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.model.Model#clearStatistics()
     */
    public void clearStatistics() {

        if (isTrain() == true) {
            CommonOps.set(sumx, 0.0);
            CommonOps.set(sumxsq, 0.0);
            numSamples = 0;
        }
    }

    /** @return the mean (mu) of the Gaussian density. */
    public double[] getMean() {

        DenseMatrix64F tmp = new DenseMatrix64F(mean);
        return tmp.getData();
    }

    /** @return the variance (sigma squared) of the Gaussian density. */
    public double[] getVariance() {

        DenseMatrix64F tmp = new DenseMatrix64F(variance);
        return tmp.getData();
    }

    public void setMean(D1Matrix64F mean) {
        this.mean = mean;
    }

    public void setVariance(D1Matrix64F variance) {
        this.variance = variance;

        /* Update log Gaussian constant. */
        MatrixOps.elementLog(this.variance, tmpArray);
        const2 = const1 - CommonOps.elementSum(tmpArray) / 2.0;
    }

    /** @return the standard deviation (sigma) of the Gaussian density. */
    public double[] getStd() {

        DenseMatrix64F std = new DenseMatrix64F(numElements, 1);
        MatrixOps.elementSquareRoot(variance, std);
        return std.getData();
    }

    /** @return the sum of the observed vectors. */
    public double[] getSumX() {

        DenseMatrix64F tmp = new DenseMatrix64F(sumx);
        return tmp.getData();
    }

    /** @return the sum of the observed vectors squared. */
    public double[] getSumXSq() {

        DenseMatrix64F tmp = new DenseMatrix64F(sumxsq);
        return tmp.getData();
    }

    /** @return the number of observations. */
    public double getNumSamples() {

        return numSamples;
    }

    /** @return the dimensionality. */
    public int getNumElements() {

        return numElements;
    }

    /** @return true if the covariance matrix is diagonal. */
    public boolean isDiagonal() {

        return isDiagonal;
    }

    /**
     * @return the value of the parameters and sufficient statistics of this
     *         model in a printable format.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder("");
        sb.append("Gaussian Model\n");
        sb.append("const: " + const2 + "\n");

        sb.append("num samp: " + numSamples + "\n");

        sb.append("mean: " + mean.toString() + "\n");
        sb.append("var: " + variance.toString() + "\n");
        sb.append("sumx: " + sumx.toString() + "\n");
        sb.append("sunxsq: " + sumxsq.toString() + "\n");

        return sb.toString();
    }
}
