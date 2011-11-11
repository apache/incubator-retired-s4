package org.apache.s4.deploy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;

/**
 * Fetches S4R files from a file system, possibly distributed.
 * 
 */
public class FileSystemS4RFetcher implements S4RFetcher {

    @Override
    public InputStream fetch(URI uri) throws DeploymentFailedException {
        try {
            return new FileInputStream(new File(uri));
        } catch (FileNotFoundException e) {
            throw new DeploymentFailedException("Cannot retrieve file from uri [" + uri.toString() + "]");
        }
    }
}
