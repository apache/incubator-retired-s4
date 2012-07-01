package org.apache.s4.core.window;

/**
 * A convenience window slot, that aggregates elements of type <T> into elements of type <U>.
 *
 * @param <T>
 *            elements to aggregate
 * @param <U>
 *            aggregated elements (can be a list, or can be a result of some processing on the elements)
 */
public interface Slot<T, U> {

    /**
     * Add a single data element
     */
    void addData(T data);

    /**
     * Retrieve all stored elements s
     */
    U getAggregatedData();

}
