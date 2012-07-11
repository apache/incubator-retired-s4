package org.apache.s4.base.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Helper class for creating S4RLoader instances for a given S4R file.
 * 
 */
public class S4RLoaderFactory {

    private static Logger logger = LoggerFactory.getLogger(S4RLoaderFactory.class);

    @Inject(optional = true)
    @Named("s4.tmp.dir")
    File tmpDir;

    /**
     * Explodes the s4r archive in a user specified directory through "s4.tmpdir" parameter, and prepares a classloader
     * that will load classes and resources from, first, the application classes, then the dependencies.
     * 
     * Inspired from Hadoop's application classloading implementation (RunJar class).
     * 
     * @param s4rPath
     *            path to s4r
     * @return classloader that loads resources from the s4r in a predefined order
     */
    public S4RLoader createS4RLoader(String s4rPath) {
        if (tmpDir == null) {
            tmpDir = Files.createTempDir();
            tmpDir.deleteOnExit();
            logger.warn(
                    "s4.tmp.dir not specified, using temporary directory [{}] for unpacking S4R. You should rather specify a non-temporary directory.",
                    tmpDir.getAbsolutePath());
        }
        JarFile jar = null;
        try {
            jar = new JarFile(s4rPath);
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    File to = new File(tmpDir, entry.getName());
                    Files.createParentDirs(to);
                    InputStream is = jar.getInputStream(entry);
                    OutputStream os = new FileOutputStream(to);
                    try {
                        ByteStreams.copy(is, os);
                    } finally {
                        Closeables.closeQuietly(is);
                        Closeables.closeQuietly(os);
                    }
                }
            }

            List<URL> classpath = new ArrayList<URL>();
            addDirLibsToClassPath(classpath, "/app");
            addDirLibsToClassPath(classpath, "/lib");
            classpath.add(new File(tmpDir.getAbsolutePath() + "/").toURI().toURL());

            S4RLoader s4rLoader = new S4RLoader(classpath.toArray(new URL[] {}));
            return s4rLoader;

        } catch (IOException e) {
            logger.error("Cannot process S4R [{}]: {}", s4rPath, e.getClass().getName() + "/" + e.getMessage());
            throw new RuntimeException("Cannot create S4R classloader", e);
        }
    }

    private void addDirLibsToClassPath(List<URL> classpath, String dir) throws MalformedURLException {
        File[] libs = new File(tmpDir, dir).listFiles();
        if (libs != null) {
            for (int i = 0; i < libs.length; i++) {
                if (!libs[i].isDirectory()) {
                    classpath.add(libs[i].toURI().toURL());
                }
            }
        }
    }
}
