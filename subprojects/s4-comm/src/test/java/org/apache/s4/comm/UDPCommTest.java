package org.apache.s4.comm;

import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Emitter;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class UDPCommTest extends SimpleDeliveryTest {

	@Override
	public void setup() {
		Injector injector = Guice.createInjector(new UDPCommTestModule());
		sdt = injector.getInstance(CommWrapper.class);
	}

	class UDPCommTestModule extends AbstractModule {

		protected PropertiesConfiguration config = null;

		private void loadProperties(Binder binder) {

			try {
				InputStream is = this.getClass().getResourceAsStream(
						"/s4-comm-test.properties");
				config = new PropertiesConfiguration();
				config.load(is);

				System.out.println(ConfigurationUtils.toString(config));
				Names.bindProperties(binder,
						ConfigurationConverter.getProperties(config));
			} catch (ConfigurationException e) {
				binder.addError(e);
				e.printStackTrace();
			}
		}

		@Override
		protected void configure() {
			if (config == null)
				loadProperties(binder());

			int numHosts = config.getList("cluster.hosts").size();
			boolean isCluster = numHosts > 1 ? true : false;
			bind(Boolean.class).annotatedWith(Names.named("isCluster"))
					.toInstance(Boolean.valueOf(isCluster));

			bind(Cluster.class);

			bind(Assignment.class).to(AssignmentFromFile.class);

			bind(Topology.class).to(TopologyFromFile.class);

			/* Use a simple UDP comm layer implementation. */
			bind(Listener.class).to(UDPListener.class);
			bind(Emitter.class).to(UDPEmitter.class);

			/* The hashing function to map keys top partitions. */
			bind(Hasher.class).to(DefaultHasher.class);

			/* Use Kryo to serialize events. */
			bind(SerializerDeserializer.class).to(KryoSerDeser.class);

			bind(Integer.class).annotatedWith(
					Names.named("emitter.send.interval")).toInstance(
					config.getInt("emitter.send.interval"));
		}
	}
}
