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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProgressListener;
import org.gradle.tooling.ProjectConnection;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class Deploy extends S4ArgsBase {

    private static File tmpAppsDir;
    static org.slf4j.Logger logger = LoggerFactory.getLogger(Deploy.class);

    /**
     * @param args
     */
    public static void main(String[] args) {

        DeployAppArgs deployArgs = new DeployAppArgs();

        Tools.parseArgs(deployArgs, args);

        try {
            ZkClient zkClient = new ZkClient(deployArgs.zkConnectionString, deployArgs.timeout);
            zkClient.setZkSerializer(new ZNRecordSerializer());

            tmpAppsDir = Files.createTempDir();

            if (!Strings.isNullOrEmpty(deployArgs.s4rPath) && !Strings.isNullOrEmpty(deployArgs.generatedS4R)) {
                logger.error("-s4r and -generatedS4R options are mutually exclusive");
                System.exit(1);
            }

            File s4rToDeploy = null;

            if (deployArgs.s4rPath != null) {
                // 1. if there is an application archive, we use it
                s4rToDeploy = new File(deployArgs.s4rPath);
                if (!s4rToDeploy.exists()) {
                    logger.error("Specified S4R file does not exist in {}", s4rToDeploy.getAbsolutePath());
                    System.exit(1);
                } else {
                    logger.info(
                            "Using specified S4R [{}], the S4R archive will not be built from source (and corresponding parameters are ignored)",
                            s4rToDeploy.getAbsolutePath());
                }
            } else if (deployArgs.gradleBuildFile != null) {

                // 2. otherwise if there is a build file, we create the S4R archive from that

                List<String> params = new ArrayList<String>();
                // prepare gradle -P parameters, including passed gradle opts
                params.addAll(deployArgs.gradleOpts);
                params.add("appClass=" + deployArgs.appClass);
                params.add("appsDir=" + tmpAppsDir.getAbsolutePath());
                params.add("appName=" + deployArgs.appName);
                ExecGradle.exec(deployArgs.gradleBuildFile, "installS4R", params.toArray(new String[] {}));
                File tmpS4R = new File(tmpAppsDir.getAbsolutePath() + "/" + deployArgs.appName + ".s4r");
                if (!Strings.isNullOrEmpty(deployArgs.generatedS4R)) {
                    logger.info("Copying generated S4R to [{}]", deployArgs.generatedS4R);
                    s4rToDeploy = new File(deployArgs.generatedS4R);
                    if (!(ByteStreams.copy(Files.newInputStreamSupplier(tmpS4R),
                            Files.newOutputStreamSupplier(s4rToDeploy)) > 0)) {
                        logger.error("Cannot copy generated s4r from {} to {}", tmpS4R.getAbsolutePath(),
                                s4rToDeploy.getAbsolutePath());
                        System.exit(1);
                    }
                } else {
                    s4rToDeploy = tmpS4R;
                }
            } else {
                if (!Strings.isNullOrEmpty(deployArgs.appClass)) {
                    // 3. otherwise if there is at least an app class specified (e.g. for running "s4 adapter"), we use
                    // it and won't use an S4R
                    logger.info("No S4R path specified, nor build file specified: this assumes the app is in the classpath");
                } else {
                    logger.error("You must specify an S4R file, a build file to create an S4R from, or an appClass that will be in the classpath");
                    System.exit(1);
                }

            }

            DeploymentUtils.initAppConfig(
                    new AppConfig.Builder().appName(deployArgs.appName)
                            .appURI(s4rToDeploy != null ? s4rToDeploy.toURI().toString() : null)
                            .appClassName(deployArgs.appClass).customModulesNames(deployArgs.modulesClassesNames)
                            .customModulesURIs(deployArgs.modulesURIs)
                            .namedParameters(convertListArgsToMap(deployArgs.extraNamedParameters)).build(),
                    deployArgs.clusterName, false, deployArgs.zkConnectionString);
            // Explicitly shutdown the JVM since Gradle leaves non-daemon threads running that delay the termination
            System.exit(0);
        } catch (Exception e) {
            LoggerFactory.getLogger(Deploy.class).error("Cannot deploy app", e);
        }

    }

    private static Map<String, String> convertListArgsToMap(List<String> args) {
        Map<String, String> result = Maps.newHashMap();
        for (String arg : args) {
            String[] split = arg.split("[=]");
            if (!(split.length == 2)) {
                throw new RuntimeException("Invalid args: " + Arrays.toString(args.toArray(new String[] {})));
            }
            result.put(split[0], split[1]);
        }
        return result;
    }

    @Parameters(commandNames = "s4 deploy", commandDescription = "Package and deploy application to S4 cluster", separators = "=")
    static class DeployAppArgs extends S4ArgsBase {

        @Parameter(names = { "-b", "-buildFile" }, description = "Full path to gradle build file for the S4 application", required = false, converter = FileConverter.class, validateWith = FileExistsValidator.class)
        File gradleBuildFile;

        @Parameter(names = "-s4r", description = "Path to existing s4r file", required = false)
        String s4rPath;

        @Parameter(names = { "-generatedS4R", "-g" }, description = "Location of generated s4r (incompatible with -s4r option). By default, s4r is generated in a temporary directory on the local file system. In a distributed environment, you probably want to specify a location accessible through a distributed file system like NFS. That's the purpose of this option.", required = false)
        String generatedS4R;

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

        @Parameter(names = { "-namedStringParameters", "-p" }, description = "Comma-separated list of inline configuration parameters, taking precedence over homonymous configuration parameters from configuration files. Syntax: '-p=name1=value1,name2=value2 '", hidden = false, converter = InlineConfigParameterConverter.class)
        List<String> extraNamedParameters = new ArrayList<String>();

    }

    /**
     * Parameters parsing utility.
     * 
     */
    public static class InlineConfigParameterConverter implements IStringConverter<String> {

        @Override
        public String convert(String arg) {
            Pattern parameterPattern = Pattern.compile("(\\S+=\\S+)");
            logger.info("processing inline configuration parameter {}", arg);
            Matcher parameterMatcher = parameterPattern.matcher(arg);
            if (!parameterMatcher.find()) {
                throw new IllegalArgumentException("Cannot understand parameter " + arg);
            }
            return parameterMatcher.group(1);
        }
    }

    static class ExecGradle {

        public static void exec(File buildFile, String taskName, String[] params) throws Exception {

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
                buildArgs.add("-info");
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
