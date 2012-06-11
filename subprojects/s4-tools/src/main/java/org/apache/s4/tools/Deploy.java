package org.apache.s4.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

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

            File s4rToDeploy = File.createTempFile("testapp" + System.currentTimeMillis(), "s4r");

            String s4rPath = null;

            if (deployArgs.s4rPath != null) {
                s4rPath = deployArgs.s4rPath;
                logger.info(
                        "Using specified S4R [{}], the S4R archive will not be built from source (and corresponding parameters are ignored)",
                        s4rPath);
            } else {
                List<String> params = new ArrayList<String>();
                // prepare gradle -P parameters, including passed gradle opts
                params.addAll(deployArgs.gradleOpts);
                params.add("appClass=" + deployArgs.appClass);
                params.add("appsDir=" + tmpAppsDir.getAbsolutePath());
                params.add("appName=" + deployArgs.appName);
                ExecGradle.exec(deployArgs.gradleBuildFile, "installS4R", params.toArray(new String[] {}));
                s4rPath = tmpAppsDir.getAbsolutePath() + "/" + deployArgs.appName + ".s4r";
            }
            Assert.assertTrue(ByteStreams.copy(Files.newInputStreamSupplier(new File(s4rPath)),
                    Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

            final String uri = s4rToDeploy.toURI().toString();
            ZNRecord record = new ZNRecord(String.valueOf(System.currentTimeMillis()));
            record.putSimpleField(DistributedDeploymentManager.S4R_URI, uri);
            zkClient.create("/s4/clusters/" + deployArgs.clusterName + "/apps/" + deployArgs.appName, record,
                    CreateMode.PERSISTENT);
            logger.info("uploaded application [{}] to cluster [{}], using zookeeper znode [{}]", new String[] {
                    deployArgs.appName, deployArgs.clusterName,
                    "/s4/clusters/" + deployArgs.clusterName + "/apps/" + deployArgs.appName });

        } catch (Exception e) {
            LoggerFactory.getLogger(Deploy.class).error("Cannot deploy app", e);
        }

    }

    @Parameters(commandNames = "s4 deploy", commandDescription = "Package and deploy application to S4 cluster", separators = "=")
    static class DeployAppArgs extends S4ArgsBase {

        @Parameter(names = { "-b", "-buildFile" }, description = "Full path to gradle build file for the S4 application", required = false, converter = FileConverter.class, validateWith = FileExistsValidator.class)
        File gradleBuildFile;

        @Parameter(names = "-s4r", description = "Path to s4r file", required = false)
        String s4rPath;

        @Parameter(names = { "-a", "-appClass" }, description = "Full class name of the application class (extending App or AdapterApp)", required = false)
        String appClass = "";

        @Parameter(names = "-appName", description = "Name of S4 application. This will be the name of the s4r file as well", required = true)
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
