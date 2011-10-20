package org.apache.s4.base;

/**
 * 
 * Get a byte array received by a lower level layer.
 * 
 */
public interface Listener {

	/*
	 * Perform blocking receive on the appropriate communication channel
	 * 
	 * @return
	 * <ul><li> byte[] message returned by the channel </li>
	 * <li> null if the associated blocking thread is interrupted </li>
	 * </ul>
	 */
	byte[] recv();

	public int getPartitionId();
}
