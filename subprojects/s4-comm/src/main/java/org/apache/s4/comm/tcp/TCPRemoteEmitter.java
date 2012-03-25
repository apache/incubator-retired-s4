package org.apache.s4.comm.tcp;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.topology.RemoteTopology;

import com.google.inject.Inject;

public class TCPRemoteEmitter extends TCPEmitter implements RemoteEmitter {

    @Inject
    public TCPRemoteEmitter(RemoteTopology topology) throws InterruptedException {
        super(topology);
    }

}
