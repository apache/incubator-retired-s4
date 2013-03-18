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

package org.apache.s4.tools;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ParsingUtils;
import org.apache.s4.deploy.DeploymentUtils;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProgressListener;
import org.gradle.tooling.ProjectConnection;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Strings;

/**
 * Deploys and S4 application configuration into the cluster manager
 */
public class Deploy extends S4ArgsBase {

    static org.slf4j.Logger logger = LoggerFactory.getLogger(Deploy.class);

    public static void main(String[] args) {

        DeployAppArgs deployArgs = new DeployAppArgs();

        Tools.parseArgs(deployArgs, args);

        try {
            ZkClient zkClient = new ZkClient(deployArgs.zkConnectionString, deployArgs.timeout);
            zkClient.setZkSerializer(new ZNRecordSerializer());

            URI s4rURI = null;

            if (deployArgs.s4rPath != null) {
                s4rURI = new URI(deployArgs.s4rPath);
                if (Strings.isNullOrEmpty(s4rURI.getScheme())) {
                    // default is file
                    s4rURI = new File(deployArgs.s4rPath).toURI();
                }
                logger.info("Using specified S4R [{}]", s4rURI.toString());
            } else {
                if (!Strings.isNullOrEmpty(deployArgs.appClass)) {
                    // 3. otherwise if there is at least an app class specified (e.g. for running "s4 adapter"), we use
                    // it and won't use an S4R
                    logger.info("No S4R path specified, nor build file specified: this assumes the app is in the classpath");
                } else {
                    logger.error("You must specify an S4R file or an appClass that will be in the classpath");
                    System.exit(1);
                }

            }

            DeploymentUtils.initAppConfig(
                    new AppConfig.Builder().appName(deployArgs.appName)
                            .appURI(s4rURI == null ? null : s4rURI.toString())
                            .customModulesNames(deployArgs.modulesClassesNames)
                            .customModulesURIs(deployArgs.modulesURIs).appClassName(deployArgs.appClass)
                            .namedParameters(ParsingUtils.convertListArgsToMap(deployArgs.extraNamedParameters))
                            .build(), deployArgs.clusterName, false, deployArgs.zkConnectionString);
            // Explicitly shutdown the JVM since Gradle leaves non-daemon threads running that delay the termination
            if (!deployArgs.testMode) {
                System.exit(0);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(Deploy.class).error("Cannot deploy app", e);
        }

    }

    @Parameters(commandNames = "s4 deploy", commandDescription = "Package and deploy application to S4 cluster", separators = "=")
    static class DeployAppArgs extends S4ArgsBase {

        @Parameter(names = "-s4r", description = "URI to existing s4r file", required = false)
        String s4rPath;

        @Parameter(names = { "-a", "-appClass" }, description = "Full class name of the application class (extending App or AdapterApp)", required = false)
        String appClass = "";

        @Parameter(names = "-appName", description = "Name of S4 application.", required = true)
        String appName;

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = "-timeout", description = "Connection timeout to Zookeeper, in ms")
        int timeout = 10000;

        @Parameter(names = { "-modulesURIs", "-mu" }, description = "URIs for fetching code of custom modules")
        List<String> modulesURIs = new ArrayList<String>();

        @Parameter(names = { "-modulesClasses", "-emc", "-mc" }, description = "Fully qualified class names of custom modules")
        List<String> modulesClassesNames = new ArrayList<String>();

        @Parameter(names = { "-namedStringParameters", "-p" }, description = "Comma-separated list of inline configuration parameters. Syntax: '-p=name1=value1,name2=value2 '", hidden = false, converter = ParsingUtils.InlineConfigParameterConverter.class)
        List<String> extraNamedParameters = new ArrayList<String>();

        @Parameter(names = "-testMode", description = "Special mode for regression testing", hidden = true)
        boolean testMode = false;

        @Parameter(names = "-debug", description = "Display debug information from the build system", arity = 0)
        boolean debug = false;
    }

    static class ExecGradle {

        public static void exec(File buildFile, String taskName, String[] params, boolean debug) throws Exception {

            ProjectConnection connection = GradleConnector.newConnector()
                    .forProjectDirectory(buildFile.getParentFile()).connect();

            try {
                BuildLauncher build = connection.newBuild();

                // select tasks to run:
                build.forTasks(taskName);

                List<String> buildArgs = new ArrayList<String>();
                // buildArgs.add("-b");
                // buildArgs.add(buildFilePath);
                buildArgs.add("-stacktrace");
                if (debug) {
                    buildArgs.add("-debug");
                }
                if (params.length > 0) {
                    for (int i = 0; i < params.length; i++) {
                        buildArgs.add("-P" + params[i]);
                    }
                }

                logger.info(Arrays.toString(buildArgs.toArray()));

                build.withArguments(buildArgs.toArray(new String[] {}));

                // if you want to listen to the progress events:
                ProgressListener listener = null; // use your implementation
                // build.addProgressListener(listener);

                // kick the build off:
                build.run();
            } finally {
                connection.close();
            }
        }
    }
}
