package io.s4.wordcount;

import io.s4.base.Emitter;
import io.s4.base.Hasher;
import io.s4.base.Listener;
import io.s4.base.SerializerDeserializer;
import io.s4.comm.DefaultHasher;
import io.s4.comm.serialize.KryoSerDeser;
import io.s4.comm.topology.Assignment;
import io.s4.comm.topology.AssignmentFromFile;
import io.s4.comm.topology.Cluster;
import io.s4.comm.topology.Topology;
import io.s4.comm.topology.TopologyFromFile;
import io.s4.comm.udp.UDPEmitter;
import io.s4.comm.udp.UDPListener;

import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

public class WordCountModule extends AbstractModule {

    protected PropertiesConfiguration config = null;

    private void loadProperties(Binder binder) {

        try {
            InputStream is = this.getClass().getResourceAsStream("/io/s4/wordcount/wordcount.properties");
            config = new PropertiesConfiguration();
            config.load(is);
            config.setProperty("cluster.lock_dir",
                    config.getString("cluster.lock_dir").replace("{user.dir}", System.getProperty("user.dir")));
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
        bind(WordCountApp.class);
        bind(Cluster.class);
        // bind(Emitter.class).to(LoopBackEmitter.class);
        bind(Hasher.class).to(DefaultHasher.class);
        // bind(Listener.class).to(LoopBackListener.class);
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(Assignment.class).to(AssignmentFromFile.class);
        bind(Topology.class).to(TopologyFromFile.class);
        bind(Emitter.class).to(UDPEmitter.class);
        bind(Listener.class).to(UDPListener.class);

    }

}
