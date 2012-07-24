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

package org.apache.s4.base.util;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * JarResources: JarResources maps all resources included in a Zip or Jar file. Additionaly, it provides a method to
 * extract one as a blob.
 * 
 * <p>
 * CREDITS
 * 
 * <p>
 * The source code for this class was derived from <a href=
 * "http://code.google.com/p/db4o-om/source/browse/trunk/objectmanager-api/src/com/db4o/objectmanager/configuration/MultiClassLoader.java"
 * >this project</a>.
 * 
 */
public final class JarResources {

    // external debug flag
    public boolean debugOn = false;

    // jar resource mapping tables
    private Map<String, Integer> htSizes = new HashMap<String, Integer>();
    private Map<String, Object> htJarContents = new HashMap<String, Object>();

    // a jar file
    private String jarFileName;

    /**
     * creates a JarResources. It extracts all resources from a Jar into an internal hashtable, keyed by resource names.
     * 
     * @param jarFileName
     *            a jar or zip file
     */
    public JarResources(String jarFileName) {
        this.jarFileName = jarFileName;
        init();
    }

    /**
     * Extracts a jar resource as a blob.
     * 
     * @param name
     *            a resource name.
     */
    public byte[] getResource(String name) {
        return (byte[]) htJarContents.get(name);
    }

    /** initializes internal hash tables with Jar file resources. */
    private void init() {
        try {
            // extracts just sizes only.
            ZipFile zf = new ZipFile(jarFileName);
            Enumeration<? extends ZipEntry> e = zf.entries();
            while (e.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) e.nextElement();

                if (debugOn) {
                    System.out.println(dumpZipEntry(ze));
                }

                htSizes.put(ze.getName(), new Integer((int) ze.getSize()));
            }
            zf.close();

            // extract resources and put them into the hashtable.
            FileInputStream fis = new FileInputStream(jarFileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            ZipInputStream zis = new ZipInputStream(bis);
            ZipEntry ze = null;
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory()) {
                    continue;
                }

                if (debugOn) {
                    System.out.println("ze.getName()=" + ze.getName() + "," + "getSize()=" + ze.getSize());
                }

                int size = (int) ze.getSize();
                // -1 means unknown size.
                if (size == -1) {
                    size = ((Integer) htSizes.get(ze.getName())).intValue();
                }

                byte[] b = new byte[(int) size];
                int rb = 0;
                int chunk = 0;
                while (((int) size - rb) > 0) {
                    chunk = zis.read(b, rb, (int) size - rb);
                    if (chunk == -1) {
                        break;
                    }
                    rb += chunk;
                }

                // add to internal resource hashtable
                htJarContents.put(ze.getName(), b);

                if (debugOn) {
                    System.out.println(ze.getName() + "  rb=" + rb + ",size=" + size + ",csize="
                            + ze.getCompressedSize());
                }
            }
            zis.close();
        } catch (NullPointerException e) {
            System.out.println("done.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Dumps a zip entry into a string.
     * 
     * @param ze
     *            a ZipEntry
     */
    private String dumpZipEntry(ZipEntry ze) {
        StringBuffer sb = new StringBuffer();
        if (ze.isDirectory()) {
            sb.append("d ");
        } else {
            sb.append("f ");
        }

        if (ze.getMethod() == ZipEntry.STORED) {
            sb.append("stored   ");
        } else {
            sb.append("defalted ");
        }

        sb.append(ze.getName());
        sb.append("\t");
        sb.append("" + ze.getSize());
        if (ze.getMethod() == ZipEntry.DEFLATED) {
            sb.append("/" + ze.getCompressedSize());
        }

        return (sb.toString());
    }

    /**
     * Is a test driver. Given a jar file and a resource name, it trys to extract the resource and then tells us whether
     * it could or not.
     * 
     * <strong>Example</strong> Let's say you have a JAR file which jarred up a bunch of gif image files. Now, by using
     * JarResources, you could extract, create, and display those images on-the-fly.
     * 
     * <pre>
     *     ...
     *     JarResources JR=new JarResources(&quot;GifBundle.jar&quot;);
     *     Image image=Toolkit.createImage(JR.getResource(&quot;logo.gif&quot;);
     *     Image logo=Toolkit.getDefaultToolkit().createImage(
     *                   JR.getResources(&quot;logo.gif&quot;)
     *                   );
     *     ...
     * </pre>
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: java JarResources <jar file name> <resource name>");
            System.exit(1);
        }

        JarResources jr = new JarResources(args[0]);
        byte[] buff = jr.getResource(args[1]);
        if (buff == null) {
            System.out.println("Could not find " + args[1] + ".");
        } else {
            System.out.println("Found " + args[1] + " (length=" + buff.length + ").");
        }
    }

} // End of JarResources class.

