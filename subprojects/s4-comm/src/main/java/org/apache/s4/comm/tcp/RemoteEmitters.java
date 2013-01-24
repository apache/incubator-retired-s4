package org.apache.s4.comm.tcp;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.topology.Cluster;

/**
 * Manages the {@link RemoteEmitter} instances for sending messages to remote subclusters.
 * 
 */
public interface RemoteEmitters {

    public abstract RemoteEmitter getEmitter(Cluster topology);

}
