package org.apache.s4.comm.util;

import java.io.InputStream;
import java.net.URI;

/**
 * Factory for remote file fetchers depending on the access protocol.
 * 
 */
public class RemoteFileFetcher implements ArchiveFetcher {

    @Override
    public InputStream fetch(URI uri) throws ArchiveFetchException {
        String scheme = uri.getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            return new FileSystemArchiveFetcher().fetch(uri);
        }
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            return new HttpArchiveFetcher().fetch(uri);
        }
        throw new ArchiveFetchException("Unsupported protocol " + scheme);
    }
}
