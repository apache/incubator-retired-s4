/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.core;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.s4.core.util.ArchiveFetchException;
import org.apache.s4.core.util.ParametersInjectionModule;
import org.apache.s4.core.util.ParsingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

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

    private static void startNode(S4NodeArgs nodeArgs) throws InterruptedException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread {}", t.getName(), e);

            }
        });

        // inject parameter from the command line, including zk string
        Map<String, String> inlineParameters = Maps.newHashMap(ParsingUtils
                .convertListArgsToMap(nodeArgs.extraNamedParameters));
        inlineParameters.put("s4.cluster.zk_address", nodeArgs.zkConnectionString);

        Injector injector = Guice.createInjector(Modules.override(
                new BaseModule(Resources.getResource("default.s4.base.properties").openStream(), nodeArgs.clusterName))
                .with(new ParametersInjectionModule(inlineParameters)));

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
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-namedStringParameters", "-p" }, description = "Comma-separated list of "
                + "inline configuration parameters, taking precedence over homonymous configuration parameters from "
                + "configuration files. Syntax: '-p=name1=value1,name2=value2 '. "
                + "NOTE: application parameters should be injected in the application configuration/deployment step."
                + "Only parameters relevant to the node should be injected here, e.g. metrics logging configuration", hidden = false, converter = ParsingUtils.InlineConfigParameterConverter.class)
        List<String> extraNamedParameters = new ArrayList<String>();
    }
}
