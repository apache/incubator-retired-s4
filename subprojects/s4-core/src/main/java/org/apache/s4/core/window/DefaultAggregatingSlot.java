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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Window slot that keeps all data elements as a list.
 * 
 * @param <T>
 *            Type of slot elements
 */
public class DefaultAggregatingSlot<T> implements Slot<T> {

    List<T> data = null;
    boolean open = true;

    @Override
    public void update(T datum) {
        if (open) {
            if (data == null) {
                data = new ArrayList<T>();
            }
            data.add(datum);
        }
    }

    @Override
    public void close() {
        open = false;
        if (data == null) {
            data = ImmutableList.of();
        } else {
            data = ImmutableList.copyOf(data);
        }
    }

    public List<T> getAggregatedData() {
        return data == null ? (List<T>) ImmutableList.of() : data;
    }

    public static class DefaultAggregatingSlotFactory<T> implements SlotFactory<DefaultAggregatingSlot<T>> {

        @Override
        public DefaultAggregatingSlot<T> createSlot() {
            return new DefaultAggregatingSlot<T>();
        }

    }
}
