package org.apache.s4.core.util;

/**
 * Exception thrown when an archive cannot be fetched correctly
 */
public class ArchiveFetchException extends Exception {

    public ArchiveFetchException(String string) {
        super(string);
    }

    public ArchiveFetchException(String string, Throwable throwable) {
        super(string, throwable);
    }

}
