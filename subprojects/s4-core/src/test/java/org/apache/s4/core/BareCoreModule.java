package org.apache.s4.core;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.NoOpDeploymentManager;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;

/**
 * Temporary module allowing assignment from ZK, communication through Netty, and distributed deployment management,
 * until we have a better way to customize node configuration
 * 
 */
public class BareCoreModule extends AbstractModule {

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(BareCoreModule.class);

    public BareCoreModule() {
    }

    @Override
    protected void configure() {
        bind(DeploymentManager.class).to(NoOpDeploymentManager.class);
        bind(Emitter.class).toInstance(Mockito.mock(Emitter.class));
        bind(Listener.class).toInstance(Mockito.mock(Listener.class));
        bind(Receiver.class).toInstance(Mockito.mock(Receiver.class));
    }
}
