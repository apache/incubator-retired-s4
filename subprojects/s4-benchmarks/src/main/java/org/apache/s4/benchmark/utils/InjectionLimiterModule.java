package org.apache.s4.benchmark.utils;

import org.apache.s4.core.staging.RemoteSendersExecutorServiceFactory;
import org.apache.s4.core.staging.ThrottlingRemoteSendersExecutorServiceFactory;

import com.google.inject.AbstractModule;

public class InjectionLimiterModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RemoteSendersExecutorServiceFactory.class).to(ThrottlingRemoteSendersExecutorServiceFactory.class);
    }

}
