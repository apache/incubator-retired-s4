package org.apache.s4.fixtures;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.s4.core.App;
import org.apache.s4.core.Main;

import com.google.common.io.PatternFilenameFilter;

/**
 * Contains static methods that can be used in tests for things such as: - files utilities: strings <-> files
 * conversion, directory recursive delete etc... - starting local instances for zookeeper and bookkeeper - distributed
 * latches through zookeeper - etc...
 * 
 */
public class CoreTestUtils extends CommTestUtils {

    public static Process forkS4App(Class<?> moduleClass, Class<?> appClass) throws IOException, InterruptedException {
        return forkProcess(App.class.getName(), moduleClass.getName(), appClass.getName());
    }

    public static Process forkS4Node() throws IOException, InterruptedException {
        return forkProcess(Main.class.getName(), new String[] {});
    }

    public static Process forkS4Node(String[] args) throws IOException, InterruptedException {
        return forkProcess(Main.class.getName(), args);
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

    public static void callGradleTask(File gradlewFile, File buildFile, String taskName, String[] params)
            throws Exception {
    
        List<String> cmdList = new ArrayList<String>();
        cmdList.add(gradlewFile.getAbsolutePath());
        cmdList.add("-c");
        cmdList.add(gradlewFile.getParentFile().getAbsolutePath() + "/settings.gradle");
        cmdList.add("-b");
        cmdList.add(buildFile.getAbsolutePath());
        cmdList.add(taskName);
        if (params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                cmdList.add("-P" + params[i]);
            }
        }
    
        System.out.println(Arrays.toString(cmdList.toArray(new String[] {})).replace(",", ""));
        ProcessBuilder pb = new ProcessBuilder(cmdList);
    
        pb.directory(buildFile.getParentFile());
        pb.redirectErrorStream();
        final Process process = pb.start();
    
        process.waitFor();
    
        // try {
        // int exitValue = process.exitValue();
        // Assert.fail("forked process failed to start correctly. Exit code is [" + exitValue + "]");
        // } catch (IllegalThreadStateException ignored) {
        // }
    
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
    
    }
}
