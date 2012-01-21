package org.apache.s4.deploy;

/**
 * Marker interface for deployment managers. Allows to supply a no-op deployment manager through dependency injection.
 * (TODO that hack should be improved!)
 * 
 */
public interface DeploymentManager {

    void start();

}
