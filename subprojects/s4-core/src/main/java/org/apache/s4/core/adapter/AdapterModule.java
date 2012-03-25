package org.apache.s4.core.adapter;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Listener;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.tcp.TCPListener;
import org.apache.s4.comm.tcp.TCPRemoteEmitter;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.RemoteTopology;
import org.apache.s4.comm.topology.RemoteTopologyFromZK;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyFromZK;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.DistributedDeploymentManager;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

public class AdapterModule extends AbstractModule {

    InputStream configFileInputStream;
    private PropertiesConfiguration config;

    public AdapterModule(InputStream configFileInputStream) {
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

        int numHosts = config.getList("cluster.hosts").size();
        boolean isCluster = numHosts > 1 ? true : false;
        bind(Boolean.class).annotatedWith(Names.named("isCluster")).toInstance(Boolean.valueOf(isCluster));

        bind(Cluster.class);

        bind(Assignment.class).to(AssignmentFromZK.class);

        bind(Topology.class).to(TopologyFromZK.class);
        bind(RemoteTopology.class).to(RemoteTopologyFromZK.class);

        bind(RemoteEmitter.class).to(TCPRemoteEmitter.class);
        bind(Emitter.class).to(TCPEmitter.class);
        bind(Listener.class).to(TCPListener.class);

        // TODO downstream hasher

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

            System.out.println(ConfigurationUtils.toString(config));
            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));
        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

}
