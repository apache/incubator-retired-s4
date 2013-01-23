/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.core.util;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Injects String parameters from a map. Used for loading parameters outside of config files, typically parameters
 * passed through the application configuration.
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
