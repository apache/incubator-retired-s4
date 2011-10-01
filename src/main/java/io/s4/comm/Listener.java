package io.s4.comm;

/**
 * 
 * Get a byte array received by a lower level layer.
 * 
 */
public interface Listener {
    byte[] recv();
    public int getPartitionId();
}
