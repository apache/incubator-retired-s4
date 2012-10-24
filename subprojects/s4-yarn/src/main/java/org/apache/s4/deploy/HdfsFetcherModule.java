package org.apache.s4.deploy;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

/**
 * This module plugs an HDFS S4R fetcher into the node configuration.
 * 
 */
public class HdfsFetcherModule extends AbstractModule {

    @Override
    protected void configure() {
        Multibinder<S4RFetcher> s4rFetcherMultibinder = Multibinder.newSetBinder(binder(), S4RFetcher.class);
        s4rFetcherMultibinder.addBinding().to(HdfsS4RFetcher.class);
    }

}
