package org.apache.s4.core.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Window slot that keeps all data elements as a list.
 *
 * @param <T>
 *            Type of slot elements
 */
public class DefaultAggregatingSlot<T, U> implements Slot<T, U> {

    List<T> data = null;

    @Override
    public void addData(T datum) {
        if (data == null) {
            data = new ArrayList<T>();
        }
        data.add(datum);

    }

    @Override
    public U getAggregatedData() {
        return (U) ((data == null) ? Collections.emptyList() : data);
    }

}
