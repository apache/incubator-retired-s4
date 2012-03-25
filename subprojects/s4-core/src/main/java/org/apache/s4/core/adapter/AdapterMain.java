package org.apache.s4.core.adapter;

import java.io.File;
import java.io.FileInputStream;

import org.apache.s4.core.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class AdapterMain {
    private static final Logger logger = LoggerFactory.getLogger(AdapterMain.class);

    public static void main(String[] args) {

        AdapterArgs adapterArgs = new AdapterArgs();
        JCommander jc = new JCommander(adapterArgs);

        try {
            jc.parse(args);
        } catch (Exception e) {
            e.printStackTrace();
            jc.usage();
        }

        try {
            Injector injector = Guice.createInjector(new AdapterModule(new FileInputStream(new File(
                    adapterArgs.s4PropertiesFilePath))));
            Server server = injector.getInstance(Server.class);
            try {
                server.start(injector);
            } catch (Exception e) {
                logger.error("Failed to start the controller.", e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Parameters(separators = "=")
    static class AdapterArgs {

        @Parameter(names = "-moduleClass", description = "module class name")
        String moduleClass;

        @Parameter(names = "-adapterClass", description = "adapter class name")
        String adapterClass;

        @Parameter(names = "-s4Properties", description = "s4 properties file path")
        String s4PropertiesFilePath;
    }

}
