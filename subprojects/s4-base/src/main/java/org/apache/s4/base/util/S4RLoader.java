package org.apache.s4.base.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.google.common.collect.MapMaker;

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
public class S4RLoader extends MultiClassLoader {

    private final JarResources jarResource;
    private final Map<String, byte[]> generatedClassBytes = new HashMap<String, byte[]>();
    
    public S4RLoader(String jarPath) {
        jarResource = new JarResources(jarPath);
    }

    /**
     * In order to load dynamically generated classes with the same classloader than 
     * the one used for loading application classes from an s4r archive, we register these
     * generated classes and bytecode in this classloader. They can be picked later.
     * 
     */
    public void addGeneratedClassBytes(String className, byte[] classBytes) {
        generatedClassBytes.put(className, classBytes);
    }
    
    @Override
    protected byte[] loadClassBytes(String className) {
        if (generatedClassBytes.containsKey(className)) {
            // note: no need to keep that data any longer
            return generatedClassBytes.remove(className);
        }
        className = formatClassName(className);
        return jarResource.getResource(className);
    }

    public List<Class<?>> getClasses(String path) {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        try {
            S4RLoader jarLoader = new S4RLoader(path);
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

                        Class<?> clazz = jarLoader.loadClass(className);
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
