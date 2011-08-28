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
package io.s4.util;

import org.ejml.data.D1Matrix64F;
import org.ejml.data.DenseMatrix64F;

/**
 * Extensions to the EJML library.
 * 
 * @author Leo Neumeyer
 */
public class MatrixOps {

    /**
     * <p>
     * Performs an in-place element by element natural logarithm operation.<br>
     * <br>
     * a<sub>ij</sub> = log(a<sub>ij</sub>)
     * </p>
     *
     * @param a The matrix on which we perform the log.  Modified.
     */
    public static void elementLog( D1Matrix64F a )
    {
        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            a.set( i , Math.log(a.get(i)) );
        }
    }

    /**
     * <p>
     * Performs an element by element natural logarithm operation.<br>
     * <br>
     * b<sub>ij</sub> = log(a<sub>ij</sub>)
     * </p>
     *
     * @param a The matrix on which we perform the log. Not Modified.
     * @param b Where the results of the operation are stored. Modified.
     */
    public static void elementLog( D1Matrix64F a , D1Matrix64F b)
    {
        if( a.numRows != b.numRows || a.numCols != b.numCols )
            throw new IllegalArgumentException("Matrices must have the same shape");

        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            b.set( i , Math.log(a.get(i)) );
        }
    }

    /**
     * <p>
     * Performs an in-place element by element square operation.<br>
     * <br>
     * a<sub>ij</sub> = a<sub>ij</sub>^2
     * </p>
     *
     * @param a The matrix on which we perform the square.  Modified.
     */
    public static void elementSquare( D1Matrix64F a )
    {
        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            a.set( i , a.get(i) * a.get(i) );
        }
    }

    /**
     * <p>
     * Performs an element by element square.<br>
     * <br>
     * b<sub>ij</sub> = a<sub>ij</sub>^2
     * </p>
     *
     * @param a The matrix on which we perform the square. Not Modified.
     * @param b Where the results of the operation are stored. Modified.
     */
    public static void elementSquare( D1Matrix64F a , D1Matrix64F b)
    {
        if( a.numRows != b.numRows || a.numCols != b.numCols )
            throw new IllegalArgumentException("Matrices must have the same shape");

        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            b.set( i , a.get(i) * a.get(i) );
        }
    }
    
    /**
     * <p>
     * Performs an in-place element by element square root operation.<br>
     * <br>
     * a<sub>ij</sub> = sqrt(a<sub>ij</sub>)
     * </p>
     *
     * @param a The matrix on which we perform the square root.  Modified.
     */
    public static void elementSquareRoot( D1Matrix64F a )
    {
        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            a.set( i , Math.sqrt(a.get(i)) );
        }
    }

    /**
     * <p>
     * Performs an element by element square root.<br>
     * <br>
     * b<sub>ij</sub> = sqrt(<sub>ij</sub>)
     * </p>
     *
     * @param a The matrix on which we perform the square root. Not Modified.
     * @param b Where the results of the operation are stored. Modified.
     */
    public static void elementSquareRoot( D1Matrix64F a , D1Matrix64F b)
    {
        if( a.numRows != b.numRows || a.numCols != b.numCols )
            throw new IllegalArgumentException("Matrices must have the same shape");

        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            b.set( i , Math.sqrt(a.get(i)) );
        }
    }
    
    /**
     * <p>
     * Set a floor value for the matrix elements.<br>
     *
     * @param floor The minimum element value.
     * @param a The input matrix. Not Modified.
     * @param b Matrix whose elements with values less than floor are set to floor. Modified.
     */
    public static void elementFloor( double floor, D1Matrix64F a , D1Matrix64F b)
    {
        if( a.numRows != b.numRows || a.numCols != b.numCols )
            throw new IllegalArgumentException("Matrices must have the same shape");

        final int size = a.getNumElements();

        for( int i = 0; i < size; i++ ) {
            if(a.get(i) < floor)
                b.set(i, floor);
            else
                b.set(i, a.get(i));
        }
    }
    
    /** Convert an array of doubles to a matrix. A new matrix is created. */
    public static D1Matrix64F arrayToMatrix(double[] data) {

        DenseMatrix64F tmp = new DenseMatrix64F(data.length, 1);
        for (int i = 0; i < data.length; i++) {
            tmp.set(i, data[i]);
        }
        return tmp;
    }
 
    /** Convert an array of doubles to a matrix passed as a parameter. */
    public static D1Matrix64F arrayToMatrix(double[] data, D1Matrix64F mat) {

        for (int i = 0; i < data.length; i++) {
            mat.set(i, data[i]);
        }
        return mat;
    }
    
}

