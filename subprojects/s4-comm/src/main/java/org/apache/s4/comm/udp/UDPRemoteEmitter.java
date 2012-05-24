package org.apache.s4.comm.udp;

import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

public class UDPRemoteEmitter extends UDPEmitter {

    /**
     * Sends to remote subclusters. This is dynamically created, through an injected factory, when new subclusters are
     * discovered (as remote streams outputs)
     */
    @Inject
    public UDPRemoteEmitter(@Assisted Cluster topology) throws InterruptedException {
        super(topology);
    }
}
