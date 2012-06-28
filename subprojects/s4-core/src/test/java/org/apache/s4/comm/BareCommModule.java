package org.apache.s4.comm;

import org.apache.s4.base.Hasher;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.core.RemoteSenders;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Default configuration module for the communication layer. Parameterizable through a configuration file.
 * 
 */
public class BareCommModule extends AbstractModule {

    public BareCommModule() {
        super();
    }

    @Override
    protected void configure() {
        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);
        /* Use Kryo to serialize events. */
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(RemoteStreams.class).toInstance(Mockito.mock(RemoteStreams.class));
        bind(RemoteSenders.class).toInstance(Mockito.mock(RemoteSenders.class));
        bind(RemoteEmitters.class).toInstance(Mockito.mock(RemoteEmitters.class));
        bind(RemoteEmitterFactory.class).toInstance(Mockito.mock(RemoteEmitterFactory.class));
        bind(Clusters.class).toInstance(Mockito.mock(Clusters.class));
        Names.bindProperties(binder(), ImmutableMap.of("cluster.name", "testCluster"));
    }

}
