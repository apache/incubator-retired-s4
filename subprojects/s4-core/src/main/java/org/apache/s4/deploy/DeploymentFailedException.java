package org.apache.s4.deploy;

/**
 * 
 * <p>
 * Indicates that an application failed to be deployed to an S4 node.
 * </p>
 * <p>
 * Thrown during detection, fetching, loading or startup of applications deployed from a repository.
 * </p>
 */
public class DeploymentFailedException extends Exception {

    public DeploymentFailedException(String message, Throwable e) {
        super(message, e);
    }

    public DeploymentFailedException(String message) {
        super(message);
    }

}
