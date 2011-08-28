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
package io.s4.model;

/**
 * Title: Array
 *
 * Description: Implements a collection of array processing methods.
 *
 *
 * @author Leo Neumeyer
 */

import java.util.StringTokenizer;

public final  class Array {

  /**
   * Don't let anyone instantiate this class.
   *
   */
  private Array() {}

  /**
   * Set array values to zero.
   *
   * @param   inArray the input array
   * @return  the cleared array
   */
  public static float[] clear(float[] inArray) {

    for(int i=0; i<inArray.length; i++) {
      inArray[i] = 0.0f;
    }
  return inArray;
  }

  /**
   * Create array with constant values.
   *
   * @return  the new array
   */
  public static float[] create(float constant, int length) {

    float[] array = new float[length];
    for(int i=0; i<length; i++) {
      array[i] = constant;
    }

    return array;
  }

  /**
   * Create array from string. String is a set of float values
   * separated by white space.
   *
   * @return  the new array.
   */
  public static float[] create(String inString, int length) {

    /* Cretae a float array. */
    float[] array = new float[length];

    /* Parse string. */
    StringTokenizer st = new StringTokenizer(inString);
    int i=0;
    while (st.hasMoreTokens() && i<length) {
      array[i++] = Float.parseFloat(st.nextToken());
    }

    return array;
  }

  /**
   * Fill array with constant value.
   *
   * @param   constant the constant value.
   * @param   inArray input array.
   * @return  the result.
   */
  public static float[] fill(float constant, float[] inArray) {

    for(int i=0; i<inArray.length; i++) {
      inArray[i] = constant;
    }
  return inArray;
  }

  /**
   * Fill array subset with constant value.
   *
   * @param   constant the constant value.
   * @param   inArray input array.
   * @param   start the start index of the subset.
   * @param   length the length of the subset.
   * @return  the result.
   */
  public static float[] fill(float constant, float[] inArray,
    int start, int length) {

    for(int i=start; i<start+length; i++) {
      inArray[i] = constant;
    }
  return inArray;
  }

  /**
   * Add two arrays. Use length of first input array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @param   outArray output array
   * @return  the added array
   */
  public static float[] add(float[] inArray1,
                          float[] inArray2,
                          float[] outArray) {

    int length = inArray1.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] + inArray2[i];
    }
    return outArray;
  }

  /**
   * Add two arrays. Create and return a new array. Use length of first input
   * array.
   *
   * @param   array1 input array
   * @param   array2 input array
   * @return  the added array
   */
  public static float[] add(float[] inArray1, float[] inArray2) {

    int length = inArray1.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] + inArray2[i];
    }
    return outArray;
  }

  /**
   * Subtract inArray1 - inArray2. Use length of first input array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @param   outArray output array
   * @return  the subtracted array
   */
  public static float[] subtract(float[] inArray1,
                          float[] inArray2,
                          float[] outArray) {

    int length = inArray1.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] - inArray2[i];
    }
    return outArray;
  }

  /**
   * Subtract inArray1 - inArray2. Create and return a new array. Use length of
   * first input array.
   *
   * @param   array1 input array
   * @param   array2 input array
   * @return  the subtracted array
   */
  public static float[] subtract(float[] inArray1, float[] inArray2) {

    int length = inArray1.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] - inArray2[i];
    }
    return outArray;
  }

  /**
   * Multiply components of inArray1 and inArray2. Use length of first input
   * array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @param   outArray output array
   * @return  the multiplied array
   */
  public static float[] multiply(float[] inArray1,
                          float[] inArray2,
                          float[] outArray) {

    int length = inArray1.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] * inArray2[i];
    }
    return outArray;
  }

  /**
   * Multiply components of inArray1 and inArray2. Create and return a new
   * array. Use length of first input array.
   *
   * @param   array1 input array
   * @param   array2 input array
   * @return  the multiplied array
   */
  public static float[] multiply(float[] inArray1, float[] inArray2) {

    int length = inArray1.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] * inArray2[i];
    }
    return outArray;
  }

  /**
   * Divide components of inArray1 by inArray2. Use length of first input
   * array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @param   outArray output array
   * @return  the divided array
   */
  public static float[] divide(float[] inArray1,
                          float[] inArray2,
                          float[] outArray) {

    int length = inArray1.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] / inArray2[i];
    }
    return outArray;
  }

  /**
   * Divide components of inArray1 by inArray2. Create and return a new
   * array. Use length of first input array.
   *
   * @param   array1 input array
   * @param   array2 input array
   * @return  the divided array
   */
  public static float[] divide(float[] inArray1, float[] inArray2) {

    int length = inArray1.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] / inArray2[i];
    }
    return outArray;
  }

  //////////////////////

  /**
   * Floor - set the minimum value for the array.
   *
   * @param   floorValue the floor value
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] floor(float floorValue,
                              float[] inArray,
                              float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      if(inArray[i] < floorValue) {
	outArray[i] = floorValue;
      } else {
	outArray[i] = inArray[i];
      }
    }
    return outArray;
  }

  /**
   * Add constant to array.
   *
   * @param   constant the constant
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] constantAdd(float constant,
                                    float[] inArray,
                                    float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray[i] + constant;
    }
    return outArray;
  }

  /**
   * Add constant to array. Create and return a new array.
   *
   * @param   constant the constant
   * @param   inArray input array
   * @return  the result
   */
  public static float[] constantAdd(float constant, float[] inArray) {

    int length = inArray.length;
    float[] outArray = new float[length];

    return constantAdd(constant, inArray, outArray);
  }


  /**
   * Multiply array components by a scale factor.
   *
   * @param   scaleFactor the scale factor
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] scale(float scaleFactor,
                              float[] inArray,
                              float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray[i] * scaleFactor;
    }
    return outArray;
  }

  /**
   * Multiply array components by a scale factor. Create and return a new array.
   *
   * @param   constant the constant
   * @param   inArray input array
   * @return  the result
   */
  public static float[] scale(float scaleFactor, float[] inArray) {

    int length = inArray.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray[i] * scaleFactor;
    }
    return outArray;
  }

  /**
   * Dot product of two arrays. Use length of first input array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] dotProduct(float[] inArray1,
                                   float[] inArray2,
                                   float[] outArray) {

    int length = inArray1.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] * inArray2[i];
    }
    return outArray;
  }

  /**
   * Dot product of two arrays. Use length of first input array.
   * Create and return a new array.
   *
   * @param   inArray1 input array
   * @param   inArray2 input array
   * @return  the result
   */
  public static float[] dotProduct(float[] inArray1, float[] inArray2) {

    int length = inArray1.length;
    float[] outArray = new float[length];
    for(int i=0; i<length; i++) {
      outArray[i] = inArray1[i] * inArray2[i];
    }
    return outArray;
  }

  /**
   * Duplicate array.
   *
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] duplicate(float[] inArray,
                                  float[] outArray) {

    int length = inArray.length;
    System.arraycopy(inArray, 0, outArray, 0, length);

    return outArray;
  }

  /**
   * Duplicate array.
   * Create and return a new array.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float[] duplicate(float[] inArray) {

    int length = inArray.length;
    float[] outArray = new float[length];
    System.arraycopy(inArray, 0, outArray, 0, length);

    return outArray;
  }

  /**
   * Square array component values.
   * Create and return a new array.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float[] square(float[] inArray) {

    float[] outArray = new float[inArray.length];
    return square(inArray, outArray);
  }

  /**
   * Square array component values.
   *
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] square(float[] inArray,
                               float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = inArray[i] * inArray[i];
    }
    return outArray;
  }

  /**
   * Square root of array component values.
   * Create and return a new array.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float[] sqrt(float[] inArray) {

    float[] outArray = new float[inArray.length];
    return sqrt(inArray, outArray);
  }

  /**
   * Square root of array component values.
   *
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] sqrt(float[] inArray,
                             float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = (float)Math.sqrt(inArray[i]);
    }
    return outArray;
  }

  /**
   * Log of array component values.
   * Create and return a new array.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float[] log(float[] inArray) {

    float[] outArray = new float[inArray.length];
    return log(inArray, outArray);
  }

  /**
   * Log of array component values.
   *
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] log(float[] inArray, float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = (float)Math.log(inArray[i]);
    }
    return outArray;
  }

  /**
   * Exp of array component values.
   * Create and return a new array.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float[] exp(float[] inArray) {

    float[] outArray = new float[inArray.length];
    return exp(inArray, outArray);
  }

  /**
   * Exp of array component values.
   *
   * @param   inArray input array
   * @param   outArray output array
   * @return  the result
   */
  public static float[] exp(float[] inArray, float[] outArray) {

    int length = inArray.length;
    for(int i=0; i<length; i++) {
      outArray[i] = (float)Math.exp(inArray[i]);
    }
    return outArray;
  }

  /**
   * Sum array component values.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float sum(float[] inArray) {

    float sum = 0.0f;
    int length = inArray.length;
    for(int i=0; i<length; i++) {
      sum += inArray[i];
    }
    return sum;
  }

  /**
   * Sum square of array component values.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float sumSquared(float[] inArray) {

    float sumsq = 0.0f;
    int length = inArray.length;
    for(int i=0; i<length; i++) {
      sumsq += inArray[i] * inArray[i];
    }
    return sumsq;
  }

  /**
   * Index of max value.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static int maxIndex(float[] inArray) {

    float max = -Float.MAX_VALUE;
    int maxIndex = 0;
    int length = inArray.length;
    for(int i=0; i<length; i++) {
      if(inArray[i] > max) {
	max = inArray[i];
	maxIndex = i;
      }
    }
    return maxIndex;
  }

  /**
   * Max value.
   *
   * @param   inArray input array
   * @return  the result
   */
  public static float maxValue(float[] inArray) {

    float max = -Float.MAX_VALUE;
    int length = inArray.length;
    for(int i=0; i<length; i++) {
      if(inArray[i] > max) {
	max = inArray[i];
      }
    }
    return max;
  }

  /**
   * Print array.
   *
   * @param   inArray the input array
   */
  public static void print(float[] inArray) {

    //System.out.print("Array Length: " + inArray.length + "\n");
    System.out.print("Array: ");
    for(int i=0; i<inArray.length; i++) {
      System.out.print(inArray[i] + " ");
    }
    System.out.print("\n");
  }

  /**
   * Print array.
   *
   * @param   inArray a 2d input array
   */
  public static void print(float[][] inArray) {

    System.out.print("Array Length: " + inArray.length +
      " X " + inArray[0].length + "\n");

    for(int i=0; i<inArray.length; i++) {
      System.out.print("Row " + i + ": ");
      for(int j=0; j<inArray[0].length; j++) {
        System.out.print(inArray[i][j] + " ");
      }
      System.out.print("\n");
    }
  }

  /**
   * Convert array to a string.
   *
   * @param   inArray the input array
   * @return  Returns a string representation of the array values.
   */
  public static String toString(float[] inArray) {

    StringBuffer sb = new StringBuffer();

    int numElementsMinusOne = inArray.length-1;
    for(int i=0; i<(numElementsMinusOne); i++) {
      sb.append(inArray[i] + " ");
    }

    return sb.append(inArray[numElementsMinusOne] ).toString();
  }


}
