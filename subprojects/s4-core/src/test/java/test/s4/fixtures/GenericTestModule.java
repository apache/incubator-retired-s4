package test.s4.fixtures;

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
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromFile;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.Topology;
import org.apache.s4.comm.topology.TopologyFromFile;
import org.apache.s4.comm.udp.UDPEmitter;
import org.apache.s4.comm.udp.UDPListener;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;

public abstract class GenericTestModule<T> extends AbstractModule {

    protected PropertiesConfiguration config = null;
    private final Class<?> appClass;

    protected GenericTestModule() {
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
        bind(appClass);
        bind(Cluster.class);
        bind(Hasher.class).to(DefaultHasher.class);
        bind(SerializerDeserializer.class).to(KryoSerDeser.class);
        bind(Assignment.class).to(AssignmentFromFile.class);
        bind(Topology.class).to(TopologyFromFile.class);
        bind(Emitter.class).to(UDPEmitter.class);
        bind(Listener.class).to(UDPListener.class);

    }

}
