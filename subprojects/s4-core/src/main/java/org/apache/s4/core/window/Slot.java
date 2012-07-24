package org.apache.s4.core.window;

/**
 * A convenience window slot, that aggregates elements of type <T>.
 * 
 * Users must add suitable getter methods to retrieve aggregated data.
 * 
 * @param <T>
 *            elements to aggregate
 */
public interface Slot<T> {

    /**
     * Add a single data element
     */
    void update(T data);

    /**
     * Compute aggregated data on available gathered slot data, place slot and slot data in immutable state.
     */
    void close();

}
