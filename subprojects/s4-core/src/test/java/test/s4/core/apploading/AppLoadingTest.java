package test.s4.core.apploading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.s4.core.Server;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import test.s4.fixtures.TestUtils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

/**
 * 
 * Tests packaging and deployment of an S4 app
 * 
 */
public class AppLoadingTest {

    public static final long ZOOKEEPER_PORT = 21810;
    private static Factory zookeeperServerConnectionFactory = null;
    private Process forkedApp;

    @Before
    public void prepare() throws Exception {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
        final ZooKeeper zk = TestUtils.createZkClient();
        try {
            zk.delete("/simpleAppCreated", -1);
        } catch (Exception ignored) {
        }

        zk.close();
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        TestUtils.killS4App(forkedApp);
    }

    @Ignore("fix paths")
    @Test
    public void testA() throws Exception {

        // add all classes from counter app
        File rootAppDir = new File(new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath()
                + "/s4-example/bin");
        File appFilesDir = new File(rootAppDir, "org/apache/s4/example/counter");
        generateS4RFromDirectoryContents(rootAppDir, appFilesDir, "counterExample",
                "org.apache.s4.example.counter.MyApp");

        forkedApp = TestUtils.forkS4Node();
        Thread.sleep(15000);
    }

    private void generateS4RFromDirectoryContents(File rootAppDir, File appFilesDir, String s4rName, String appClassName)
            throws IOException, FileNotFoundException {
        Collection<File> s4rFiles = listFilesRecursively(appFilesDir);
        File jarFile = new File(System.getProperty("user.dir") + "/bin/apps/" + s4rName + ".s4r");
        Files.createParentDirs(jarFile);
        FileOutputStream fos = new FileOutputStream(jarFile);
        JarOutputStream jos = new JarOutputStream(fos);
        System.out.println(System.getProperty("java.class.path"));
        for (File file : s4rFiles) {
            JarEntry jarEntry = new JarEntry(file.getAbsolutePath().substring(rootAppDir.getAbsolutePath().length()));
            jos.putNextEntry(jarEntry);
            ByteStreams.copy(Files.newInputStreamSupplier(file), jos);
        }
        // add manifest
        File manifest = File.createTempFile("s4app", "manifest");
        String manifestContents = "Manifest-Version: 1.0\n" + Server.MANIFEST_S4_APP_CLASS + ": " + appClassName + "\n";
        Files.write(manifestContents, manifest, Charset.forName("UTF-8"));
        JarEntry jarEntry = new JarEntry("META-INF/MANIFEST.MF");
        jos.putNextEntry(jarEntry);
        ByteStreams.copy(Files.newInputStreamSupplier(manifest), jos);

        jos.close();
    }

    private Collection<File> listFilesRecursively(File dir) {
        if (dir.isDirectory()) {
            File[] listFiles = dir.listFiles();
            List<File> filesToAdd = new ArrayList<File>();
            if (listFiles.length != 0) {
                for (File file : listFiles) {
                    if (file.isFile()) {
                        filesToAdd.add(file);
                    } else if (file.isDirectory()) {
                        filesToAdd.addAll(listFilesRecursively(file));
                    }
                }
            }
            return filesToAdd;
        } else {
            // TODO throw exception
            return null;
        }
    }

    /**
     * 
     * 1. generates an s4r package from classes in the apploading package (TODO process still to be improved), 2.
     * deploys it to bin/apps 3. starts a forked S4 node, which loads apps from bin/apps 4. verifies app is working (s4
     * app started, event correctly processed)
     * 
     * NOTE: we'll need to add an automatic test for which we make sure code cannot be in the classpath
     */
    @Test
    public void testAppLoading() throws Exception {

        // TODO fix paths

        final ZooKeeper zk = TestUtils.createZkClient();

        File rootAppDir = TestUtils.findDirForCompiledTestClasses();

        File appFilesDir = new File(rootAppDir, "test/s4/core/apploading");
        // 1. create app jar and place it in tmp/s4-apps
        generateS4RFromDirectoryContents(rootAppDir, appFilesDir, "appLoadingTest", SimpleApp.class.getName());

        CountDownLatch signalAppStarted = new CountDownLatch(1);
        // 2. start s4 node and check results
        forkedApp = TestUtils.forkS4Node();

        // TODO wait for ready state (zk node available)
        Thread.sleep(5000);

        // note: make sure we don't delete existing znode if it was already created
        TestUtils.watchAndSignalCreation("/simpleAppCreated", signalAppStarted, zk, false);

        Assert.assertTrue(signalAppStarted.await(20, TimeUnit.SECONDS));

        String time1 = String.valueOf(System.currentTimeMillis());

        CountDownLatch signalEvent1Processed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/onEvent@" + time1, signalEvent1Processed, zk);

        TestUtils.injectIntoStringSocketAdapter(time1);

        // check event processed
        Assert.assertTrue(signalEvent1Processed.await(5, TimeUnit.SECONDS));

    }

}
