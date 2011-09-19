package io.s4.comm;

public interface Emitter {
    void send(int partitionId, byte[] message);
    int getPartitionCount();
}
