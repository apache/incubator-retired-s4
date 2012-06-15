package org.apache.s4.comm.tcp;

import java.util.HashMap;
import java.util.Map;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.RemoteEmitterFactory;
import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Manages the {@link RemoteEmitter} instances for sending messages to remote subclusters.
 * 
 */
@Singleton
public class RemoteEmitters {

    Map<Cluster, RemoteEmitter> emitters = new HashMap<Cluster, RemoteEmitter>();

    @Inject
    RemoteEmitterFactory emitterFactory;

    public RemoteEmitter getEmitter(Cluster topology) {
        RemoteEmitter emitter = emitters.get(topology);
        if (emitter == null) {
            emitter = emitterFactory.createRemoteEmitter(topology);
            emitters.put(topology, emitter);
        }
        return emitter;
    }

}
