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

package org.apache.s4.fixtures;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.BaseModule;
import org.apache.s4.core.DefaultCoreModule;
import org.apache.s4.core.S4Node;
import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

import com.google.common.io.PatternFilenameFilter;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

/**
 * Contains static methods that can be used in tests for things such as: - files utilities: strings <-> files
 * conversion, directory recursive delete etc... - starting local instances for zookeeper and bookkeeper - distributed
 * latches through zookeeper - etc...
 * 
 */
public class CoreTestUtils extends CommTestUtils {

    public static Process forkS4Node() throws IOException, InterruptedException {
        ZkClient zkClient = new ZkClient("localhost:2181");
        int waitTimeInSeconds = 10;
        return forkS4Node(new String[] {}, zkClient, waitTimeInSeconds, "cluster1");
    }

    public static Process forkS4Node(String[] args, ZkClient zkClient, int waitTimeInSeconds, String clusterName)
            throws IOException, InterruptedException {
        return forkS4Node(-1, args, zkClient, waitTimeInSeconds, clusterName);
    }

    public static Process forkS4Node(int debugPort, String[] args, ZkClient zkClient, int waitTimeInSeconds,
            String clusterName) throws IOException, InterruptedException {

        final CountDownLatch signalNodeReady = new CountDownLatch(1);

        zkClient.subscribeChildChanges("/s4/clusters/" + clusterName + "/process", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() == 1) {
                    signalNodeReady.countDown();
                }

            }
        });
        Process forked = forkProcess(S4Node.class.getName(), debugPort, args);

        Assert.assertTrue(signalNodeReady.await(waitTimeInSeconds, TimeUnit.SECONDS));

        // having picked a partition does not mean having downloaded and loaded the platform and app code. Need to wait
        // a bit more
        // TODO add a zookeeper node for indicating node ready state?
        Thread.sleep(1000);

        return forked;
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
            // ProgressListener listener = null; // use your implementation

            // kick the build off:
            build.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Injector createInjectorWithNonFailFastZKClients() throws IOException {
        return Guice.createInjector(Modules.override(
                new BaseModule(Resources.getResource("default.s4.base.properties").openStream(), "cluster1"),
                new DefaultCommModule(Resources.getResource("default.s4.comm.properties").openStream()),
                new DefaultCoreModule(Resources.getResource("default.s4.core.properties").openStream())).with(
                new NonFailFastZookeeperClientsModule()));
    }

}
