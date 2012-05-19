package org.apache.s4.core;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Listener;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.RemoteEmitterFactory;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.tcp.TCPListener;
import org.apache.s4.comm.tcp.TCPRemoteEmitter;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterFromZK;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.ClustersFromZK;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.DistributedDeploymentManager;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * Temporary module allowing assignment from ZK, communication through Netty, and distributed deployment management,
 * until we have a better way to customize node configuration
 * 
 */
public class DefaultModule extends AbstractModule {

    InputStream configFileInputStream;
    private PropertiesConfiguration config;

    public DefaultModule(InputStream configFileInputStream) {
        this.configFileInputStream = configFileInputStream;
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        if (configFileInputStream != null) {
            try {
                configFileInputStream.close();
            } catch (IOException ignored) {
            }
        }

        bind(Emitter.class).to(TCPEmitter.class);

        bind(Listener.class).to(TCPListener.class);

        bind(Assignment.class).to(AssignmentFromZK.class);

        bind(Clusters.class).to(ClustersFromZK.class);
        bind(Cluster.class).to(ClusterFromZK.class);

        // RemoteEmitter instances are created through a factory, depending on the topology. We inject the factory
        install(new FactoryModuleBuilder().implement(RemoteEmitter.class, TCPRemoteEmitter.class).build(
                RemoteEmitterFactory.class));
        bind(RemoteEmitters.class);

        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);

        /* Use Kryo to serialize events. */
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);

        bind(DeploymentManager.class).to(DistributedDeploymentManager.class);
    }

    private void loadProperties(Binder binder) {
        try {
            config = new PropertiesConfiguration();
            config.load(configFileInputStream);

            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));
        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

}
