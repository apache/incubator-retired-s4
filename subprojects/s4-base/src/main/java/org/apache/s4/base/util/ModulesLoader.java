package org.apache.s4.base.util;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A classloader for loading module classes from a list of URLs, typically locally copied/extracted files.
 * 
 */
public class ModulesLoader extends URLClassLoader {

    public ModulesLoader(URL[] urls) {
        super(urls);
    }

}
