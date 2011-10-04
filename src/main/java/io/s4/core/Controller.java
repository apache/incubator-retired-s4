package io.s4.core;

import java.io.File;
import java.net.URL;

import org.jboss.modules.LocalModuleLoader;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

class Controller {

    final private String moduleName;
    final private String logLevel;

    /**
     * 
     */
    @Inject
    public Controller(@Named("comm.module") String moduleName,
            @Named("s4.logger_level") String logLevel) {
        this.moduleName = moduleName;
        this.logLevel = logLevel;
    }

    private static final Logger logger = LoggerFactory
            .getLogger(Controller.class);

    public void start() throws Exception {

        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        Injector injector;
        AbstractModule module = null;

        /* Initialize communication layer module. */
        try {
            module = (AbstractModule) Class.forName(moduleName).newInstance();
        } catch (Exception e) {
            logger.error("Unable to instantiate communication layer module.", e);
        }

        /* After some indirection we get the injector. */
        injector = Guice.createInjector(module);

        // HERE WE SHOULD LOOP TO CHECK IF WE NEED TO LOAD OR UNLOAD APPS.
        
        logger.trace("Load HelloApp");
        
        /* The root dir for the modules is called "modules". */
        File repoRoot = new File(getClass().getClassLoader().getResource("modules").toURI());
        
        /* Create a module loader using the root. */
        ModuleLoader moduleLoader = new LocalModuleLoader(new File[] {repoRoot});
        
        /* Load teh module and run! */
        moduleLoader.loadModule(ModuleIdentifier.fromString("helloapp")).run(new String[] {});
        
//        org.jboss.modules.Module mod = moduleLoader.loadModule(ModuleIdentifier.fromString("modules.helloapp"));
//        System.out.println(mod.toString());
//        System.out.println(mod.getClassLoader().toString());
//        mod.run(new String[] {});
        
        // /*
        // * Create the Event Generator objects using injection. The generators
        // * will be serialized and sent to remote hosts.
        // */
        // List<EventGenerator> generators = injector.getInstance(Key
        // .get(new TypeLiteral<List<EventGenerator>>() {
        // }));
        //
        // /*
        // * The communicator interface hides the implementation details of how
        // * the EventGenerator instance is sent to remote hosts.
        // */
        // Communicator comm = injector.getInstance(Communicator.class);

    }
}
