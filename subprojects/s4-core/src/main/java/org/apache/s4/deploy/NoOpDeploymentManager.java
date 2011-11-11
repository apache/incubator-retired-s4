package org.apache.s4.deploy;

/**
 * Does not handle any deployment (hence does not require any cluster configuration settings)
 * 
 */
public class NoOpDeploymentManager implements DeploymentManager {

    @Override
    public void start() {
        // does nothing
    }
}
