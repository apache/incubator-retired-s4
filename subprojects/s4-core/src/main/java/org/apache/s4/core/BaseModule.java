package org.apache.s4.core;

import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.AssignmentFromHelix;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterFromHelix;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.comm.util.RemoteFileFetcher;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.HelixBasedDeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class BaseModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(BaseModule.class);

    private PropertiesConfiguration config;
    InputStream baseConfigInputStream;
    String clusterName;
    private String instanceName;
    boolean useHelix = true;

    public BaseModule(InputStream baseConfigInputStream, String clusterName,
            String instanceName) {
        super();
        this.baseConfigInputStream = baseConfigInputStream;
        this.clusterName = clusterName;
        this.instanceName = instanceName;
    }

    @Override
    protected void configure() {
        if (config == null) {
            loadProperties(binder());
        }
        if (useHelix) {
            bind(Assignment.class).to(AssignmentFromHelix.class)
                    .asEagerSingleton();
            bind(Cluster.class).to(ClusterFromHelix.class);
            bind(DeploymentManager.class).to(HelixBasedDeploymentManager.class).in(Scopes.SINGLETON);

            bind(ArchiveFetcher.class).to(RemoteFileFetcher.class);
            bind(Bootstrap.class).to(S4HelixBootstrap.class);

            // share the Zookeeper connection
            bind(ZkClient.class).toProvider(ZkClientProvider.class).in(
                    Scopes.SINGLETON);
            return;
        }
        // a node holds a single partition assignment
        // ==> Assignment is a singleton so it shared between base, comm and app
        // layers.
        // it is eager so that the node is able to join a cluster immediately
        bind(Assignment.class).to(AssignmentFromZK.class).asEagerSingleton();
        // bind(Cluster.class).to(ClusterFromZK.class);

        bind(ArchiveFetcher.class).to(RemoteFileFetcher.class);
        bind(Bootstrap.class).to(S4Bootstrap.class);

        // share the Zookeeper connection
        bind(ZkClient.class).toProvider(ZkClientProvider.class).in(
                Scopes.SINGLETON);

    }

    @SuppressWarnings("serial")
    private void loadProperties(Binder binder) {
        try {
            config = new PropertiesConfiguration();
            config.load(baseConfigInputStream);

            // TODO - validate properties.

            /* Make all properties injectable. Do we need this? */
            Names.bindProperties(binder,
                    ConfigurationConverter.getProperties(config));

            if (clusterName != null) {
                if (config.containsKey("s4.cluster.name")) {
                    logger.warn(
                            "cluster [{}] passed as a parameter will not be used because an existing cluster.name parameter of value [{}] was found in the configuration file and will be used",
                            clusterName, config.getProperty("s4.cluster.name"));
                } else {
                    Names.bindProperties(binder, new HashMap<String, String>() {
                        {
                            put("s4.cluster.name", clusterName);
                        }
                    });
                }
            }
            if (instanceName != null) {
                if (config.containsKey("s4.instance.name")) {
                    logger.warn(
                            "instanceName [{}] passed as a parameter will not be used because an existing s4.instance.name parameter of value [{}] was found in the configuration file and will be used",
                            instanceName,
                            config.getProperty("s4.instance.name"));
                } else {
                    Names.bindProperties(binder, new HashMap<String, String>() {
                        {
                            put("s4.instance.name", instanceName);
                        }
                    });
                }
            }

        } catch (ConfigurationException e) {
            binder.addError(e);
            e.printStackTrace();
        }
    }
}
