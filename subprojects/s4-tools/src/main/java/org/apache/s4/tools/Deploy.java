package org.apache.s4.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DistributedDeploymentManager;
import org.apache.zookeeper.CreateMode;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.slf4j.LoggerFactory;

import sun.net.ProgressListener;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class Deploy {

    private static File tmpAppsDir;
    static org.slf4j.Logger logger = LoggerFactory.getLogger(Deploy.class);

    /**
     * @param args
     */
    public static void main(String[] args) {

        DeployAppArgs deployArgs = new DeployAppArgs();

        Tools.parseArgs(deployArgs, args);
        // configure log4j for Zookeeper
        BasicConfigurator.configure();
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        Logger.getLogger("org.I0Itec").setLevel(Level.ERROR);

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
                ExecGradle.exec(deployArgs.gradleExecPath, deployArgs.gradleBuildFilePath, "installS4R", new String[] {
                        "appsDir=" + tmpAppsDir.getAbsolutePath(), "appName=" + deployArgs.appName });
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

        @Parameter(names = "-gradle", description = "path to gradle/gradlew executable", required = false)
        String gradleExecPath;

        @Parameter(names = "-buildFile", description = "path to gradle build file for the S4 application", required = false)
        String gradleBuildFilePath;

        @Parameter(names = "-s4r", description = "path to s4r file", required = false)
        String s4rPath;

        @Parameter(names = "-appName", description = "name of S4 application", required = true)
        String appName;

        @Parameter(names = "-cluster", description = "logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = "-zk", description = "zookeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = "-timeout", description = "connection timeout to Zookeeper, in ms")
        int timeout = 10000;

    }

    static class ExecGradle {

        public static void exec(String gradlewExecPath, String buildFilePath, String taskName, String[] params)
                throws Exception {

            ProjectConnection connection = GradleConnector.newConnector()
                    .forProjectDirectory(new File(buildFilePath).getParentFile()).connect();

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
