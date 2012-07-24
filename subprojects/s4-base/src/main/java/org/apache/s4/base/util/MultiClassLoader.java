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
 * "http://www.javaworld.com/javaworld/jw-10-1996/jw-10-indepth.html?page=1" >article by Chuck McManis</a>.
 * 
 * 
 * Thank you to the authors!
 */
abstract public class MultiClassLoader extends ClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(MultiClassLoader.class);

    private final Map<String, Class<?>> classes;
    private char classNameReplacementChar;

    public MultiClassLoader() {
        classes = new MapMaker().makeMap();
    }

    // ---------- Superclass Overrides ------------------------
    /**
     * This is a simple version for external clients since they will always want the class resolved before it is
     * returned to them.
     */
    @Override
    public Class<?> loadClass(String className) throws ClassNotFoundException {
        return (loadClass(className, true));
    }

    @Override
    public synchronized Class<?> loadClass(String className, boolean resolveIt) throws ClassNotFoundException {

        Class<?> result;
        byte[] classBytes;
        logger.trace("MultiClassLoader loadClass - className: " + className + ", resolveIt: " + resolveIt);

        /* Check our local cache of classes. */
        result = classes.get(className);
        if (result != null) {
            logger.trace("Returning cached result for class [{}]", className);
            return result;
        }

        /* Check with the primordial class loader. */
        try {
            result = super.findSystemClass(className);
            logger.trace("Returning system class (in CLASSPATH) [{}]", className);
            return result;
        } catch (ClassNotFoundException e) {
            logger.trace("Not a system class [{}]", className);
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
        logger.trace("Returning newly loaded class [{}]", className);
        return result;
    }

    /**
     * This optional call allows a class name such as "COM.test.Hello" to be changed to "COM_test_Hello", which is
     * useful for storing classes from different packages in the same retrieval directory. In the above example the char
     * would be '_'.
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
