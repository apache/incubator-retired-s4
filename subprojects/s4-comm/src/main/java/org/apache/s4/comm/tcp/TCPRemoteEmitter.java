package org.apache.s4.comm.tcp;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;

/**
 * Emitter to remote subclusters.
 * 
 */
public class TCPRemoteEmitter extends TCPEmitter implements RemoteEmitter {

    /**
     * Sends to remote subclusters. This is dynamically created, through an
     * injected factory, when new subclusters are discovered (as remote streams
     * outputs)
     */
    @Inject
    public TCPRemoteEmitter(@Assisted Cluster topology, @Named("comm.timeout") int timeout)
            throws InterruptedException {
        super(topology, timeout);
    }

}
