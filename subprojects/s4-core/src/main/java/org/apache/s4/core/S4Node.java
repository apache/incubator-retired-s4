package org.apache.s4.core;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.s4.comm.util.ArchiveFetchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Entry point for starting an S4 node. It parses arguments and injects an {@link S4Bootstrap} based on the
 * {@link BaseModule} minimal configuration.
 * 
 */
public class S4Node {

    private static Logger logger = LoggerFactory.getLogger(S4Node.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        S4NodeArgs s4Args = new S4NodeArgs();
        JCommander jc = new JCommander(s4Args);

        try {
            jc.parse(args);
        } catch (Exception e) {
            JCommander.getConsole().println("Cannot parse arguments: " + e.getMessage());
            jc.usage();
            System.exit(1);
        }
        startNode(s4Args);

    }

    private static void startNode(S4NodeArgs mainArgs) throws InterruptedException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread {}", t.getName(), e);

            }
        });

        Injector injector = Guice.createInjector(new Module[] { new BaseModule(Resources.getResource(
                "default.s4.base.properties").openStream(), mainArgs.clusterName) });
        S4Bootstrap bootstrap = injector.getInstance(S4Bootstrap.class);
        try {
            bootstrap.start(injector);
        } catch (ArchiveFetchException e1) {
            logger.error("Cannot fetch module dependencies.", e1);
        }
    }

    /**
     * Defines command parameters.
     * 
     */
    @Parameters(separators = "=")
    public static class S4NodeArgs {

        @Parameter(names = { "-c", "-cluster" }, description = "Cluster name", required = true)
        String clusterName = null;

        @Parameter(names = "-baseConfig", description = "S4 base configuration file", required = false)
        String baseConfigFilePath = null;

        @Parameter(names = "-zk", description = "Zookeeper connection string", required = false)
        String zkConnectionString;

    }
}
