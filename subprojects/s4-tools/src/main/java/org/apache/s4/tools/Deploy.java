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

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DistributedDeploymentManager;
import org.apache.zookeeper.CreateMode;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProgressListener;
import org.gradle.tooling.ProjectConnection;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
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

            File s4rToDeploy;

            if (deployArgs.s4rPath != null) {
                s4rToDeploy = new File(deployArgs.s4rPath);
                if (!s4rToDeploy.exists()) {
                    logger.error("Specified S4R file does not exist in {}", s4rToDeploy.getAbsolutePath());
                    System.exit(1);
                } else {
                    logger.info(
                            "Using specified S4R [{}], the S4R archive will not be built from source (and corresponding parameters are ignored)",
                            s4rToDeploy.getAbsolutePath());
                }
            } else {
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
            }

            final String uri = s4rToDeploy.toURI().toString();
            ZNRecord record = new ZNRecord(String.valueOf(System.currentTimeMillis()));
            record.putSimpleField(DistributedDeploymentManager.S4R_URI, uri);
            record.putSimpleField("name", deployArgs.appName);
            String deployedAppPath = "/s4/clusters/" + deployArgs.clusterName + "/app/s4App";
            if (zkClient.exists(deployedAppPath)) {
                ZNRecord readData = zkClient.readData(deployedAppPath);
                logger.error("Cannot deploy app [{}], because app [{}] is already deployed", deployArgs.appName,
                        readData.getSimpleField("name"));
                System.exit(1);
            }

            zkClient.create("/s4/clusters/" + deployArgs.clusterName + "/app/s4App", record, CreateMode.PERSISTENT);
            logger.info(
                    "uploaded application [{}] to cluster [{}], using zookeeper znode [{}], and s4r file [{}]",
                    new String[] { deployArgs.appName, deployArgs.clusterName,
                            "/s4/clusters/" + deployArgs.clusterName + "/app/" + deployArgs.appName,
                            s4rToDeploy.getAbsolutePath() });

            // Explicitly shutdown the JVM since Gradle leaves non-daemon threads running that delay the termination
            System.exit(0);
        } catch (Exception e) {
            LoggerFactory.getLogger(Deploy.class).error("Cannot deploy app", e);
        }

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
