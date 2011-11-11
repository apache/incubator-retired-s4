package org.apache.s4.deploy;

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

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

public class TestModule extends AbstractModule {

    private PropertiesConfiguration config;

    private void loadProperties(Binder binder) {

        try {
            InputStream is = this.getClass().getResourceAsStream("/org.apache.s4.deploy.s4.properties");
            config = new PropertiesConfiguration();
            config.load(is);
            System.out.println(ConfigurationUtils.toString(config));
            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder, ConfigurationConverter.getProperties(config));
        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        bind(Cluster.class);
        bind(Hasher.class).to(DefaultHasher.class);
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(Assignment.class).to(AssignmentFromZK.class);
        bind(Topology.class).to(TopologyFromZK.class);
        bind(Emitter.class).to(TCPEmitter.class);
        bind(Listener.class).to(TCPListener.class);
    }

}
