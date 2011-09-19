package io.s4.comm;

public interface Listener {
    byte[] recv();
    public int getPartitionId();
}
