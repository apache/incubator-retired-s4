package org.apache.s4.fixtures;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.core.Receiver;
import org.apache.s4.deploy.DeploymentManager;
import org.apache.s4.deploy.NoOpDeploymentManager;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;

/**
 * Core module mocking basic platform functionalities.
 *
 */
public class MockCoreModule extends AbstractModule {

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(MockCoreModule.class);

    public MockCoreModule() {
    }

    @Override
    protected void configure() {
        bind(DeploymentManager.class).to(NoOpDeploymentManager.class);
        bind(Emitter.class).toInstance(Mockito.mock(Emitter.class));
        bind(Listener.class).toInstance(Mockito.mock(Listener.class));
        bind(Receiver.class).toInstance(Mockito.mock(Receiver.class));
    }
}
