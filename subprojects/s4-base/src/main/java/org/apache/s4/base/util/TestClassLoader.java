package org.apache.s4.base.util;

import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClassLoader extends MultiClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(TestClassLoader.class);

    @Override
    /** Simple method to read a class file from a known location. */
    public byte[] loadClassBytes(String className) {

        byte[] bytes = null;
        String filename = "/tmp/" + className + ".impl";
        logger.debug("Reading: " + filename);

        FileInputStream fi = null;
        try {
            fi = new FileInputStream(filename);
            bytes = new byte[fi.available()];
            fi.read(bytes);
            return bytes;
        } catch (Exception e) {

            /*
             * If we caught an exception, either the class wasn't found or it was unreadable by our process.
             */
            logger.error("Unable to load class: {}.", filename);
            e.printStackTrace();
            return null;
        } finally {
            if (fi != null) {
                try {
                    fi.close();
                } catch (IOException e) {
                    logger.warn("Exception while closing input stream", e);
                }
            }
        }
    }

}
