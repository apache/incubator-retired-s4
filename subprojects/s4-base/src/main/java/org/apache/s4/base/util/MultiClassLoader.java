package org.apache.s4.base.util;

import java.io.FileInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

/**
 * Custom class loader to load classes in its own classloader.
 * 
 * <p>
 * CREDITS
 * 
 * <p>
 * The source code for this class was derived from <a href=
 * "http://code.google.com/p/db4o-om/source/browse/trunk/objectmanager-api/src/com/db4o/objectmanager/configuration/MultiClassLoader.java"
 * >this project</a> which was derived from this <a href=
 * "http://www.javaworld.com/javaworld/jw-10-1996/jw-10-indepth.html?page=1"
 * >article by Chuck McManis</a>.
 * 
 * 
 * Thank you to the authors!
 */
abstract public class MultiClassLoader extends ClassLoader {

    private static final Logger logger = LoggerFactory
            .getLogger(MultiClassLoader.class);

    private Map<String, Class<?>> classes;
    private char classNameReplacementChar;

    public MultiClassLoader() {
        classes = new MapMaker().makeMap();
    }

    // ---------- Superclass Overrides ------------------------
    /**
     * This is a simple version for external clients since they will always want
     * the class resolved before it is returned to them.
     */
    public Class<?> loadClass(String className) throws ClassNotFoundException {
        return (loadClass(className, true));
    }

    public synchronized Class<?> loadClass(String className, boolean resolveIt)
            throws ClassNotFoundException {

        Class<?> result;
        byte[] classBytes;
        logger.debug("MultiClassLoader loadClass - className: " + className
                + ", resolveIt: " + resolveIt);

        /* Check our local cache of classes. */
        result = (Class<?>) classes.get(className);
        if (result != null) {
            logger.debug("Returning cached result.");
            return result;
        }

        /* Check with the primordial class loader. */
        try {
            result = super.findSystemClass(className);
            logger.debug("Returning system class (in CLASSPATH).");
            return result;
        } catch (ClassNotFoundException e) {
            logger.debug("Not a system class.");
        }

        classBytes = loadClassBytes(className);
        if (classBytes == null) {
            throw new ClassNotFoundException();
        }

        /* Define it (parse the class file). */
        try {
            result = defineClass(className, classBytes, 0, classBytes.length);
            if (result == null) {
                throw new ClassFormatError();
            }
        } catch (NoClassDefFoundError ex) {
            // Dependencies were not found
            logger.warn(ex.getMessage());
        }

        /* Resolve if necessary. */
        if (resolveIt)
            resolveClass(result);

        /* Done. */
        if (result == null)
            return null;
        classes.put(className, result);
        logger.debug("Returning newly loaded class.");
        return result;
    }

    /**
     * This optional call allows a class name such as "COM.test.Hello" to be
     * changed to "COM_test_Hello", which is useful for storing classes from
     * different packages in the same retrieval directory. In the above example
     * the char would be '_'.
     */
    public void setClassNameReplacementChar(char replacement) {
        classNameReplacementChar = replacement;
    }

    /** Simple method to read a class file from a known location. */
    abstract protected byte[] loadClassBytes(String className);

    protected String formatClassName(String className) {
        if (classNameReplacementChar == '\u0000') {
            // '/' is used to map the package to the path
            return className.replace('.', '/') + ".class";
        } else {
            // Replace '.' with custom char, such as '_'
            return className.replace('.', classNameReplacementChar) + ".class";
        }
    }

    // --- Std
    protected static void print(String text) {
        System.out.println(text);
    }

}
