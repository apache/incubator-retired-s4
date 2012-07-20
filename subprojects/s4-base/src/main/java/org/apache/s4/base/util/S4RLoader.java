package org.apache.s4.base.util;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A classloader that fetches and loads classes and resources from :
 * <ul>
 * <li>Application classes in an S4R archive</li>
 * <li>Application dependencies from an S4R archive</li>
 * <li>Classes dynamically generated
 * 
 */
public class S4RLoader extends URLClassLoader {

    public S4RLoader(URL[] urls) {
        super(urls);
    }

    public Class<?> loadGeneratedClass(String name, byte[] bytes) {
        Class<?> clazz = findLoadedClass(name);
        if (clazz == null) {
            return defineClass(name, bytes, 0, bytes.length);
        } else {
            return clazz;
        }
    }

}
