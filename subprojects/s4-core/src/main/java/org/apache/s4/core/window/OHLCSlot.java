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

package org.apache.s4.core.window;

/**
 * Open - high - low -close slot as typically used in financial data and useful for summarizing data.
 */
public class OHLCSlot implements Slot<Double> {

    double open = -1;
    double high = -1;
    double low = -1;
    double close = -1;
    long ticks = 0;
    boolean isOpen;

    @Override
    public void update(Double data) {
        if (isOpen) {
            if (open == -1) {
                open = low = high = close = data;
            } else if (data > high) {
                high = data;
            } else if (data < low) {
                low = data;
            }
            close = data;
            ticks++;
        }
    }

    @Override
    public void close() {
        isOpen = false;
    }

    double getOpen() {
        return open;
    }

    double getClose() {
        return close;
    }

    double getHigh() {
        return high;
    }

    double getLow() {
        return low;
    }

    long getTicksCount() {
        return ticks;
    }

    public static class OHLCSlotFactory implements SlotFactory<OHLCSlot> {

        @Override
        public OHLCSlot createSlot() {
            return new OHLCSlot();
        }

    }

}
