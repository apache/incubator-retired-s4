package org.apache.s4.fixtures;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

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
import org.apache.s4.comm.udp.UDPEmitter;
import org.apache.s4.comm.udp.UDPListener;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

// also uses netty
public class ZkBasedClusterManagementTestModule<T> extends AbstractModule {

    protected PropertiesConfiguration config = null;
    private final Class<?> appClass;

    protected ZkBasedClusterManagementTestModule() {
        // infer actual app class through "super type tokens" (this simple code
        // assumes actual module class is a direct subclass from this one)
        ParameterizedType pt = (ParameterizedType) getClass().getGenericSuperclass();
        Type[] fieldArgTypes = pt.getActualTypeArguments();
        this.appClass = (Class<?>) fieldArgTypes[0];
    }

    private void loadProperties(Binder binder) {

        try {
            InputStream is = this.getClass().getResourceAsStream("/default.s4.properties");
            config = new PropertiesConfiguration();
            config.load(is);
            config.setProperty("cluster.zk_address",
                    config.getString("cluster.zk_address").replaceFirst("\\w+:\\d+", "localhost:" + TestUtils.ZK_PORT));
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
        bind(appClass);
        bind(Cluster.class);
        bind(Hasher.class).to(DefaultHasher.class);
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(Assignment.class).to(AssignmentFromZK.class);
        bind(Topology.class).to(TopologyFromZK.class);
        bind(Emitter.class).to(TCPEmitter.class);
        bind(Listener.class).to(TCPListener.class);
    }

}
