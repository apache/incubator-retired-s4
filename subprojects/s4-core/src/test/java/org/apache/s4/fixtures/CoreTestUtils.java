package org.apache.s4.fixtures;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.s4.core.Main;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

import sun.net.ProgressListener;

import com.google.common.io.PatternFilenameFilter;

/**
 * Contains static methods that can be used in tests for things such as: - files utilities: strings <-> files
 * conversion, directory recursive delete etc... - starting local instances for zookeeper and bookkeeper - distributed
 * latches through zookeeper - etc...
 * 
 */
public class CoreTestUtils extends CommTestUtils {

    public static Process forkS4Node() throws IOException, InterruptedException {
        return forkS4Node(new String[] {});
    }

    public static Process forkS4Node(String[] args) throws IOException, InterruptedException {
        return forkS4Node(-1, args);
    }

    public static Process forkS4Node(int debugPort, String[] args) throws IOException, InterruptedException {
        return forkProcess(Main.class.getName(), debugPort, args);
    }

    public static File findGradlewInRootDir() {
        File gradlewFile = null;
        if (new File(System.getProperty("user.dir")).listFiles(new PatternFilenameFilter("gradlew")).length == 1) {
            gradlewFile = new File(System.getProperty("user.dir") + File.separator + "gradlew");
        } else {
            if (new File(System.getProperty("user.dir")).getParentFile().getParentFile()
                    .listFiles(new PatternFilenameFilter("gradlew")).length == 1) {
                gradlewFile = new File(new File(System.getProperty("user.dir")).getParentFile().getParentFile()
                        .getAbsolutePath()
                        + File.separator + "gradlew");
            } else {
                Assert.fail("Cannot find gradlew executable in [" + System.getProperty("user.dir") + "] or ["
                        + new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath() + "]");
            }
        }
        return gradlewFile;
    }

    public static void callGradleTask(File buildFile, String taskName, String[] params) throws Exception {

        ProjectConnection connection = GradleConnector.newConnector().forProjectDirectory(buildFile.getParentFile())
                .connect();

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

            build.withArguments(buildArgs.toArray(new String[] {}));

            // if you want to listen to the progress events:
            ProgressListener listener = null; // use your implementation

            // kick the build off:
            build.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
