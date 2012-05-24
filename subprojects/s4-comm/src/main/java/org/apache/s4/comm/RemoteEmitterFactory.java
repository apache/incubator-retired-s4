package org.apache.s4.comm;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.topology.Cluster;

public interface RemoteEmitterFactory {

    RemoteEmitter createRemoteEmitter(Cluster topology);

}
