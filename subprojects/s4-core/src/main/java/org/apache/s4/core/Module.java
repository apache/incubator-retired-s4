package org.apache.s4.core;

import java.io.File;
import java.io.InputStream;

import javax.inject.Provider;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.DistributedDeploymentManager;
import org.apache.s4.deploy.NoOpDeploymentManager;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;

/**
 * First level of S4 configuration,
 * 
 * @author Leo Neumeyer
 */
public class Module extends AbstractModule {

    protected PropertiesConfiguration config = null;

    private void loadProperties(Binder binder) {

        try {
            InputStream is = this.getClass().getResourceAsStream("/s4-core.properties");
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
        if (config == null)
            loadProperties(binder());

        bind(Server.class).asEagerSingleton();
        bind(DeploymentManager.class).to(NoOpDeploymentManager.class);
        /*
         * Apps dir is searched as follows: The s4.apps.path property in the properties file. The user's current working
         * directory under the subdirectory /bin/apps.
         */
        String appsDir = config.getString("s4.apps.path", System.getProperty("user.dir") + File.separator + "bin"
                + File.separator + "apps");
        bind(String.class).annotatedWith(Names.named("appsDir")).toInstance(appsDir);
    }

}
