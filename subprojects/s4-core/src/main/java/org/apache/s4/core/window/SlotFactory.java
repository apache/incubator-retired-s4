package org.apache.s4.core.window;

/**
 * Defines factory for window slots
 * 
 * @param <T>
 *            slot class or interface that is produced
 */
public interface SlotFactory<T> {

    T createSlot();

}
