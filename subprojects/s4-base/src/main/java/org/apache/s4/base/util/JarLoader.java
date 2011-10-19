package org.apache.s4.base.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 
 * CREDITS
 * 
 * <p>
 * The source code for this class was derived from <a href=
 * "http://code.google.com/p/db4o-om/source/browse/trunk/objectmanager-api/src/com/db4o/objectmanager/configuration/MultiClassLoader.java"
 * >this project</a>.
 * 
 * 
 */
public class JarLoader extends MultiClassLoader {

    private JarResources jarResource;

    public JarLoader(String jarPath) {
        jarResource = new JarResources(jarPath);
    }

    @Override
    protected byte[] loadClassBytes(String className) {
        className = formatClassName(className);
        return jarResource.getResource(className);
    }

    public List<Class> getClasses(String path) {
        List<Class> classes = new ArrayList<Class>();
        try {
            JarLoader jarLoader = new JarLoader(path);
            JarFile jarFile = new JarFile(path);
            for (Enumeration<JarEntry> e = jarFile.entries(); e
                    .hasMoreElements();) {
                try {
                    JarEntry entry = e.nextElement();
                    if (entry.getName().endsWith(".class")) {
                        String className = entry
                                .getName()
                                .substring(0, entry.getName().indexOf(".class"))
                                .replace("/", ".").replace("\\", ".");

                        Class clazz = jarLoader.loadClass(className);
                        // clazz.asSubclass(IoAdapter.class);
                        if (clazz != null) {
                            classes.add(clazz);
                        }
                    }
                } catch (Exception ex) {
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return classes;
    }

}
