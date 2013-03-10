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
package org.apache.s4.comm;

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

import org.apache.s4.base.util.ModulesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class ModulesLoaderFactory {

    private static Logger logger = LoggerFactory.getLogger(ModulesLoaderFactory.class);

    @Inject
    @Named("s4.tmp.dir")
    File tmpDir;

    /**
     * Explodes the jar archive in a subdirectory of a user specified directory through "s4.tmp.dir" parameter, and
     * prepares a classloader that will load classes and resources from, first, the application classes, then the
     * dependencies.
     * 
     * Inspired from Hadoop's application classloading implementation (RunJar class).
     * 
     * @param modulesFiles
     *            files containing modules classes
     * @return classloader that loads resources from the archive in a predefined order
     */
    public ModulesLoader createModulesLoader(Iterable<File> modulesFiles) {
        List<URL> classpath = new ArrayList<URL>();
        for (File moduleFile : modulesFiles) {
            addModuleToClasspath(moduleFile, classpath);
        }
        return new ModulesLoader(classpath.toArray(new URL[] {}));

    }

    private void addModuleToClasspath(File moduleFile, List<URL> classpath) {

        File moduleDir = null;
        if (tmpDir == null) {
            moduleDir = Files.createTempDir();
            moduleDir.deleteOnExit();
            logger.warn(
                    "s4.tmp.dir not specified, using temporary directory [{}] for unpacking S4R. You may want to specify a parent non-temporary directory.",
                    moduleDir.getAbsolutePath());
        } else {
            moduleDir = new File(tmpDir, moduleFile.getName() + "-" + System.currentTimeMillis());
            if (!moduleDir.mkdir()) {
                throw new RuntimeException("Cannot create directory for unzipping S4R file in ["
                        + moduleDir.getAbsolutePath() + "]. Aborting deployment.");
            }
        }
        logger.info("Unzipping S4R archive in [{}]", moduleDir.getAbsolutePath());

        JarFile jar = null;
        try {
            jar = new JarFile(moduleFile);
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    File to = new File(moduleDir, entry.getName());
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

            classpath.add(moduleDir.toURI().toURL());
            addDirLibsToClassPath(classpath, moduleDir, "/lib");

        } catch (IOException e) {
            logger.error("Cannot process S4R [{}]: {}", moduleFile.getAbsolutePath(),
                    e.getClass().getName() + "/" + e.getMessage());
            throw new RuntimeException("Cannot create S4R classloader", e);
        }
    }

    private void addDirLibsToClassPath(List<URL> classpath, File s4rDir, String dir) throws MalformedURLException {
        File[] libs = new File(s4rDir, dir).listFiles();
        if (libs != null) {
            for (int i = 0; i < libs.length; i++) {
                if (!libs[i].isDirectory()) {
                    classpath.add(libs[i].toURI().toURL());
                }
            }
        }
    }
}
