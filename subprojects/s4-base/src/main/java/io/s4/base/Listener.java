package io.s4.base;

/**
 * 
 * Get a byte array received by a lower level layer.
 * 
 */
public interface Listener {
    byte[] recv();
    public int getPartitionId();
}
