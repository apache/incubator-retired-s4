package io.s4.base;

public interface Emitter {
    void send(int partitionId, byte[] message);
    int getPartitionCount();
}
