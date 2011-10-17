package io.s4;

import io.s4.core.App;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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

/**
 * Contains static methods that can be used in tests for things such as: - files
 * utilities: strings <-> files conversion, directory recursive delete etc... -
 * starting local instances for zookeeper and bookkeeper - distributed latches
 * through zookeeper - etc...
 * 
 */
public class TestUtils {

    public static final int ZK_PORT = 21810;
    public static final int INITIAL_BOOKIE_PORT = 5000;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
            + "storage");
    public static ServerSocket serverSocket;

    public static Process forkS4App(Class<?> moduleClass, Class<?> appClass) throws IOException, InterruptedException {

        List<String> cmdList = new ArrayList<String>();
        cmdList.add("java");
        cmdList.add("-cp");
        cmdList.add(System.getProperty("java.class.path"));
        // cmdList.add("-Xdebug");
        // cmdList.add("-Xnoagent");
        //
        // cmdList.add("-Xrunjdwp:transport=dt_socket,address=8788,server=y,suspend=n");
        cmdList.add(App.class.getName());
        cmdList.add(moduleClass.getName());
        cmdList.add(appClass.getName());

        // System.out.println(Arrays.toString(cmdList.toArray(new
        // String[]{})).replace(",", ""));

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
        if (f.exists()) {
            if (!f.delete()) {
                throw new RuntimeException("Cannot delete file " + f.getAbsolutePath());
            }
        }

        FileWriter fw = null;
        try {
            if (!f.createNewFile()) {
                throw new RuntimeException("Cannot create new file : " + f.getAbsolutePath());
            }
            fw = new FileWriter(f);

            fw.write(s);
        } catch (IOException e) {
            throw (e);
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    throw (e);
                }
            }
        }
    }

    public static String readFile(File f) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
                if (line != null) {
                    sb.append("\n");
                }
            }
            return sb.toString();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    throw (e);
                }
            }
        }

    }

    public static NIOServerCnxn.Factory startZookeeperServer() throws IOException, InterruptedException,
            KeeperException {

        List<String> cmdList = new ArrayList<String>();
        final File zkDataDir = new File(System.getProperty("user.dir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "data");
        if (zkDataDir.exists()) {
            TestUtils.deleteDirectoryContents(zkDataDir);
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
        FileReader fr = new FileReader(f);
        StringBuilder sb = new StringBuilder("");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while (line != null) {
            sb.append(line);
            line = br.readLine();
            if (line != null) {
                sb.append("\n");
            }
        }
        return sb.toString();

    }

    // TODO factor this code (see BasicFSStateStorage) - or use commons io or
    // guava
    public static byte[] readFileAsByteArray(File file) throws Exception {
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);

            long length = file.length();

            /*
             * Arrays can only be created using int types, so ensure that the
             * file size is not too big before we downcast to create the array.
             */
            if (length > Integer.MAX_VALUE) {
                throw new IOException("Error file is too large: " + file.getName() + " " + length + " bytes");
            }

            byte[] buffer = new byte[(int) length];
            int offSet = 0;
            int numRead = 0;

            while (offSet < buffer.length && (numRead = in.read(buffer, offSet, buffer.length - offSet)) >= 0) {
                offSet += numRead;
            }

            if (offSet < buffer.length) {
                throw new IOException("Error, could not read entire file: " + file.getName() + " " + offSet + "/"
                        + buffer.length + " bytes read");
            }

            in.close();
            return buffer;

        } finally {
            if (in != null) {
                in.close();
            }
        }
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

        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
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
                // ignore
            }
        }
        return false;
    }

    public static void cleanupTmpDirs() {
        if (TestUtils.DEFAULT_TEST_OUTPUT_DIR.exists()) {
            deleteDirectoryContents(TestUtils.DEFAULT_TEST_OUTPUT_DIR);
        }
        TestUtils.DEFAULT_STORAGE_DIR.mkdirs();

    }

    public static void stopSocketAdapter() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    public static void injectIntoStringSocketAdapter(String string) throws IOException {
        Socket socket = null;
        PrintWriter writer = null;
        try {
            socket = new Socket("localhost", 12000);
            writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println(string);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

}
