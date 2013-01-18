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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Contains static methods that can be used in tests for things such as: - files utilities: strings <-> files
 * conversion, directory recursive delete etc... - starting local instances for zookeeper and bookkeeper - distributed
 * latches through zookeeper - etc...
 * 
 */
public class CommTestUtils {

    private static final Logger logger = LoggerFactory.getLogger(CommTestUtils.class);

    public static final int ZK_PORT = 2181;
    public static final String ZK_STRING = "localhost:" + ZK_PORT;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
            + "storage");
    static {
        logger.info("Storage dir: " + DEFAULT_STORAGE_DIR);
    }
    public final static String MESSAGE = "message@" + System.currentTimeMillis();

    public final static CountDownLatch SIGNAL_MESSAGE_RECEIVED = new CountDownLatch(1);

    protected static Process forkProcess(String mainClass, int debugPort, String... args) throws IOException,
            InterruptedException {
        List<String> cmdList = new ArrayList<String>();
        cmdList.add("java");
        cmdList.add("-cp");
        cmdList.add(System.getProperty("java.class.path"));
        if (debugPort != -1) {
            cmdList.add("-Xdebug");
            cmdList.add("-Xnoagent");
            cmdList.add("-Xrunjdwp:transport=dt_socket,address=" + debugPort + ",server=y,suspend=n");
        }

        cmdList.add(mainClass);
        for (String arg : args) {
            cmdList.add(arg);
        }

        System.out.println(Arrays.toString(cmdList.toArray(new String[] {})).replace(",", ""));
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream();
        final Process process = pb.start();

        // TODO some synchro with s4 platform ready state
        Thread.sleep(2000);
        try {
            int exitValue = process.exitValue();
            Assert.fail("forked process failed to start correctly. Exit code is [" + exitValue + "]");
        } catch (IllegalThreadStateException ignored) {
        }

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

        return process;
    }

    public static void killS4App(Process forkedApp) throws IOException, InterruptedException {
        if (forkedApp != null) {
            forkedApp.destroy();
        }
    }

    public static void writeStringToFile(String s, File f) throws IOException {
        Files.write(s, f, Charset.defaultCharset());
    }

    public static String readFile(File f) throws IOException {
        return Files.toString(f, Charset.defaultCharset());

    }

    public static NIOServerCnxn.Factory startZookeeperServer() throws IOException, InterruptedException,
            KeeperException {

        final File zkDataDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "data");
        if (zkDataDir.exists()) {
            CommTestUtils.deleteDirectoryContents(zkDataDir);
        } else {
            zkDataDir.mkdirs();
        }

        ZooKeeperServer zks = new ZooKeeperServer(zkDataDir, zkDataDir, 3000);
        NIOServerCnxn.Factory nioZookeeperConnectionFactory = new NIOServerCnxn.Factory(new InetSocketAddress(ZK_PORT));
        nioZookeeperConnectionFactory.startup(zks);
        Assert.assertTrue("waiting for server being up", waitForServerUp("localhost", ZK_PORT, 4000));
        return nioZookeeperConnectionFactory;

    }

    public static void stopZookeeperServer(NIOServerCnxn.Factory f) throws IOException, InterruptedException {
        if (f != null) {
            f.shutdown();
            Assert.assertTrue("waiting for server down", waitForServerDown("localhost", ZK_PORT, 3000));
        }
    }

    public static void deleteDirectoryContents(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDirectoryContents(file);
            }
            if (!file.delete()) {
                throw new RuntimeException("could not delete : " + file);
            }
        }
    }

    public static String readFileAsString(File f) throws IOException {
        return Files.toString(f, Charset.defaultCharset());

    }

    public static byte[] readFileAsByteArray(File file) throws Exception {
        return Files.toByteArray(file);
    }

    public static ZooKeeper createZkClient() throws IOException {
        final ZooKeeper zk = new ZooKeeper("localhost:" + ZK_PORT, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        return zk;
    }

    public static void watchAndSignalCreation(String path, final CountDownLatch latch, final ZooKeeper zk)
            throws KeeperException, InterruptedException {

        // by default delete existing nodes with same path
        watchAndSignalCreation(path, latch, zk, false);
    }

    public static void watchAndSignalCreation(String path, final CountDownLatch latch, final ZooKeeper zk,
            boolean deleteIfExists) throws KeeperException, InterruptedException {

        if (zk.exists(path, false) != null) {
            if (deleteIfExists) {
                zk.delete(path, -1);
            } else {
                latch.countDown();
            }
        }
        zk.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (EventType.NodeCreated.equals(event.getType())) {
                    latch.countDown();
                }
            }
        });
    }

    public static void watchAndSignalChangedChildren(String path, final CountDownLatch latch, final ZooKeeper zk)
            throws KeeperException, InterruptedException {

        zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (EventType.NodeChildrenChanged.equals(event.getType())) {
                    latch.countDown();
                }
            }
        });
    }

    // from zookeeper's codebase
    public static boolean waitForServerUp(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                String result = send4LetterWord(host, port, "stat");
                if (result.startsWith("Zookeeper version:")) {
                    return true;
                }
            } catch (IOException ignored) {
                // ignore as this is expected
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
        return false;
    }

    // from zookeeper's codebase
    public static String send4LetterWord(String host, int port, String cmd) throws IOException {
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    // from zookeeper's codebase
    public static boolean waitForServerDown(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                send4LetterWord(host, port, "stat");
            } catch (IOException e) {
                return true;
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
        return false;
    }

    public static void cleanupTmpDirs() {
        if (CommTestUtils.DEFAULT_TEST_OUTPUT_DIR.exists()) {
            deleteDirectoryContents(CommTestUtils.DEFAULT_TEST_OUTPUT_DIR);
        }
        CommTestUtils.DEFAULT_STORAGE_DIR.mkdirs();

    }

    /**
     * gradle and eclipse have different directories for output files This is justified here
     * http://gradle.1045684.n5.nabble.com/Changing-default-IDE-output-directories-td3335478.html#a3337433
     * 
     * A consequence is that for tests to reference compiled files, we need to resolve the corresponding directory at
     * runtime.
     * 
     * This is what this method does
     * 
     * @return directory containing the compiled test classes for this project and execution environment.
     */
    public static File findDirForCompiledTestClasses() {
        String userDir = System.getProperty("user.dir");
        String classpath = System.getProperty("java.class.path");
        System.out.println(userDir);
        System.out.println(classpath);
        if (classpath.contains(userDir + "/bin")) {
            // eclipse classpath
            return new File(userDir + "/bin");
        } else if (classpath.contains(userDir + "/build/classes/test")) {
            // gradle classpath
            return new File(userDir + "/build/classes/test");
        } else {
            // TODO other IDEs
            throw new RuntimeException("Cannot find path for compiled test classes");
        }

    }

}
