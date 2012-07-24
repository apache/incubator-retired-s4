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

package org.apache.s4.core;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestCircularFifoBuffer extends TestCase {

    protected void setUp() {

    }

    public void test1() {

        System.out.println("Buffer size is 10.\n");
        CircularFifoBuffer<Integer> circularBuffer = new CircularFifoBuffer<Integer>(
                10);

        System.out.println("Add ints 100-114.");
        for (int i = 0; i < 15; i++) {
            circularBuffer.add(i + 100);
        }

        System.out.println("Iterate.");
        int j = 5;
        for(Integer num : circularBuffer) {
            System.out.print(num + " ");
            Assert.assertEquals(j + 100, num.intValue());
            j++;
        }
        System.out.println("\nLeast recent value: " + circularBuffer.get());
        Assert.assertEquals(105, circularBuffer.get().intValue());
        System.out.println("\n");
        
        circularBuffer.clear();
        
        /* Less than max size. */
        System.out.println("Clear and add ints 200-204.");
        for (int i = 0; i < 5; i++) {
            circularBuffer.add(i + 200);
        }
        
        System.out.println("Iterate.");
        int z = 0;
        for(Integer num : circularBuffer) {
            System.out.print(num + " ");
            Assert.assertEquals(z + 200, num.intValue());
            z++;
        }
        System.out.println("\n");
    }
}
