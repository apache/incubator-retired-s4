package org.apache.s4.base;

public interface Emitter {
	
	/*
	 * @param partitionId - destination partition
	 * @param message - message payload that needs to be sent
	 * @return - true - if message is sent across successfully
	 *         - false - if send fails
	 */
    boolean send(int partitionId, byte[] message);
    int getPartitionCount();
}
