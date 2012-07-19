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
