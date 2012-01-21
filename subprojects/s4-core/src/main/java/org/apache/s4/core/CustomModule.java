package org.apache.s4.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.DefaultHasher;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.tcp.TCPListener;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyFromZK;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.DistributedDeploymentManager;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

/**
 * Temporary module allowing assignment from ZK, communication through Netty, and distributed deployment management,
 * until we have a better way to customize node configuration
 * 
 */
public class CustomModule extends AbstractModule {

    File configFile;
    private PropertiesConfiguration config;

    public CustomModule(File customConfigFile) {
        this.configFile = customConfigFile;
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }

        int numHosts = config.getList("cluster.hosts").size();
        boolean isCluster = numHosts > 1 ? true : false;
        bind(Boolean.class).annotatedWith(Names.named("isCluster")).toInstance(Boolean.valueOf(isCluster));

        bind(Cluster.class);

        bind(Assignment.class).to(AssignmentFromZK.class);

        bind(Topology.class).to(TopologyFromZK.class);

        bind(Emitter.class).to(TCPEmitter.class);
        bind(Listener.class).to(TCPListener.class);

        /* The hashing function to map keys top partitions. */
        bind(Hasher.class).to(DefaultHasher.class);

        /* Use Kryo to serialize events. */
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);

        bind(DeploymentManager.class).to(DistributedDeploymentManager.class);
    }

    private void loadProperties(Binder binder) {
        InputStream is = null;
        try {
            is = new FileInputStream(configFile);
            config = new PropertiesConfiguration();
            config.load(is);

            System.out.println(ConfigurationUtils.toString(config));
            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));
        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            binder.addError(e);
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

}
