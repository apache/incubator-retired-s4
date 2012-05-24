package org.apache.s4.core.util;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Injects String parameters from a map. Used for loading parameters outside of config files.
 * 
 */
public class ParametersInjectionModule extends AbstractModule {

    Map<String, String> params;

    public ParametersInjectionModule(Map<String, String> params) {
        this.params = params;
    }

    @Override
    protected void configure() {
        for (Map.Entry<String, String> param : params.entrySet()) {
            bind(String.class).annotatedWith(Names.named(param.getKey())).toInstance(param.getValue());
        }

    }

}
