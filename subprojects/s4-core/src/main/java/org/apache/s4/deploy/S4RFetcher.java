package org.apache.s4.deploy;

import java.io.InputStream;
import java.net.URI;

/**
 * This interface defines methods to fetch S4R archive files from a URI. Various protocols can be supported in the
 * implementation classes (e.g. file system, HTTP etc...)
 * 
 */
public interface S4RFetcher {

    /**
     * Returns a stream to an S4R archive file
     * 
     * @param uri
     *            S4R archive identifier
     * @return an input stream for accessing the content of the S4R file
     * @throws DeploymentFailedException
     *             when fetching fails
     */
    InputStream fetch(URI uri) throws DeploymentFailedException;

}
