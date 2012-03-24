package org.apache.s4.deploy.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class DeployApp {

    private static File tmpAppsDir;

    /**
     * @param args
     */
    public static void main(String[] args) {

        DeployAppArgs appArgs = new DeployAppArgs();
        JCommander jc = new JCommander(appArgs);
        // configure log4j for Zookeeper
        BasicConfigurator.configure();
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        Logger.getLogger("org.I0Itec").setLevel(Level.ERROR);

        try {
            jc.parse(args);
        } catch (Exception e) {
            jc.usage();
            System.exit(-1);
        }
        try {
            ZkClient zkClient = new ZkClient(appArgs.zkConnectionString);
            zkClient.setZkSerializer(new ZNRecordSerializer());

            tmpAppsDir = Files.createTempDir();

            // File gradlewFile = CoreTestUtils.findGradlewInRootDir();

            // CoreTestUtils.callGradleTask(gradlewFile, new File(appArgs.gradleBuildFilePath), "installS4R",
            // new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });
            ExecGradle.exec(appArgs.gradleExecPath, appArgs.gradleBuildFilePath, "installS4R",
                    new String[] { "appsDir=" + tmpAppsDir.getAbsolutePath() });

            File s4rToDeploy = File.createTempFile("testapp" + System.currentTimeMillis(), "s4r");

            Assert.assertTrue(ByteStreams.copy(Files.newInputStreamSupplier(new File(tmpAppsDir.getAbsolutePath() + "/"
                    + appArgs.appName + ".s4r")), Files.newOutputStreamSupplier(s4rToDeploy)) > 0);

            final String uri = s4rToDeploy.toURI().toString();
            ZNRecord record = new ZNRecord(String.valueOf(System.currentTimeMillis()));
            record.putSimpleField(DistributedDeploymentManager.S4R_URI, uri);
            zkClient.create("/" + appArgs.clusterName + "/apps/" + appArgs.appName, record, CreateMode.PERSISTENT);

        } catch (Exception e) {
            LoggerFactory.getLogger(DeployApp.class).error("Cannot deploy app", e);
        }

    }

    @Parameters(separators = "=")
    static class DeployAppArgs {

        @Parameter(names = "-gradle", description = "path to gradle/gradlew executable", required = true)
        String gradleExecPath;

        @Parameter(names = "-buildFile", description = "path to gradle build file for the S4 application", required = true)
        String gradleBuildFilePath;

        @Parameter(names = "-appName", description = "name of S4 application", required = true)
        String appName;

        @Parameter(names = "-cluster", description = "logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = "-zk", description = "zookeeper connection string")
        String zkConnectionString = "localhost:2181";

    }

    static class ExecGradle {

        public static void exec(String gradlewExecPath, String buildFilePath, String taskName, String[] params)
                throws Exception {
            List<String> cmdList = new ArrayList<String>();
            cmdList.add(gradlewExecPath);
            // cmdList.add("-c");
            // cmdList.add(gradlewFile.getParentFile().getAbsolutePath() + "/settings.gradle");
            cmdList.add("-b");
            cmdList.add(buildFilePath);
            cmdList.add(taskName);
            if (params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    cmdList.add("-P" + params[i]);
                }
            }

            System.out.println(Arrays.toString(cmdList.toArray(new String[] {})).replace(",", ""));
            ProcessBuilder pb = new ProcessBuilder(cmdList);

            pb.directory(new File(buildFilePath).getParentFile());
            pb.redirectErrorStream();

            final Process process = pb.start();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;
                    try {
                        line = br.readLine();
                        while (line != null) {
                            System.out.println(line);
                            line = br.readLine();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
            process.waitFor();

            // try {
            // int exitValue = process.exitValue();
            // Assert.fail("forked process failed to start correctly. Exit code is [" + exitValue + "]");
            // } catch (IllegalThreadStateException ignored) {
            // }

        }
    }

}
