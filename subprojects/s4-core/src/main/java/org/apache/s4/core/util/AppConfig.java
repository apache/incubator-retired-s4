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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.s4.comm.topology.ZNRecord;

/**
 * Container for application parameters, with facilities to write and read the configuration from ZooKeeper.
 * <p>
 * Can be constructed through a builder pattern.
 */
public class AppConfig {

    public static final String NAMED_PARAMETERS = "namedParams";
    public static final String APP_CLASS = "appClass";
    public static final String APP_NAME = "appName";
    public static final String APP_URI = "S4R_URI";
    public static final String MODULES_CLASSES = "modulesClasses";
    public static final String MODULES_URIS = "modulesURIs";

    String appName;
    String appClassName;
    List<String> customModulesNames = Collections.emptyList();
    List<String> customModulesURIs = Collections.emptyList();
    Map<String, String> namedParameters = Collections.emptyMap();
    String appURI;

    private AppConfig() {
    }

    public AppConfig(ZNRecord znRecord) {
        appName = znRecord.getSimpleField(APP_NAME);
        appClassName = znRecord.getSimpleField(APP_CLASS);
        appURI = znRecord.getSimpleField(APP_URI);
        customModulesNames = znRecord.getListField(MODULES_CLASSES);
        customModulesURIs = znRecord.getListField(MODULES_URIS);
        namedParameters = znRecord.getMapField(NAMED_PARAMETERS);
    }

    public AppConfig(String appName, String appClassName, String appURI, List<String> customModulesNames,
            List<String> customModulesURIs, Map<String, String> namedParameters) {
        super();
        this.appName = appName;
        this.appClassName = appClassName;
        this.appURI = appURI;
        this.customModulesNames = customModulesNames;
        this.customModulesURIs = customModulesURIs;
        this.namedParameters = namedParameters;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppClassName() {
        return appClassName;
    }

    public String getAppURI() {
        return appURI;
    }

    public List<String> getCustomModulesNames() {
        return customModulesNames;
    }

    public List<String> getCustomModulesURIs() {
        return customModulesURIs;
    }

    public Map<String, String> getNamedParameters() {
        return namedParameters;
    }

    public String getNamedParametersAsString() {
        if (namedParameters == null || namedParameters.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> param : namedParameters.entrySet()) {
            sb.append(param.getKey() + "=" + param.getValue() + ",");
        }
        return sb.toString();
    }

    public ZNRecord asZNRecord(String id) {
        ZNRecord record = new ZNRecord(id);
        if (appClassName != null) {
            record.putSimpleField(APP_CLASS, appClassName);
        }
        if (appName != null) {
            record.putSimpleField(APP_NAME, appName);
        }
        if (appURI != null) {
            record.putSimpleField(APP_URI, appURI);
        }
        if (customModulesNames != null) {
            record.putListField(MODULES_CLASSES, customModulesNames);
        }
        if (customModulesURIs != null) {
            record.putListField(MODULES_URIS, customModulesURIs);
        }
        if (namedParameters != null) {
            record.putMapField(NAMED_PARAMETERS, namedParameters);
        }
        return record;
    }

    @Override
    public String toString() {
        return "app name: [" + appName + "] \n " + "app class: [" + appClassName + "] \n" + "app URI : [" + appURI
                + "] \n" + "modules classes : [" + customModulesNames == null ? ""
                : (Arrays.toString(customModulesNames.toArray(new String[] {}))) + " \n" + "modules URIs ["
                        + customModulesURIs == null ? ""
                        : (Arrays.toString(customModulesURIs.toArray(new String[] {}))) + "]";
    }

    public static class Builder {

        AppConfig config;

        public Builder() {
            this.config = new AppConfig();
        }

        public Builder appName(String appName) {
            config.appName = appName;
            return this;
        }

        public Builder appClassName(String appClassName) {
            config.appClassName = appClassName;
            return this;
        }

        public Builder appURI(String appURI) {
            config.appURI = appURI;
            return this;
        }

        public Builder customModulesNames(List<String> customModulesNames) {
            if (customModulesNames != null) {
                config.customModulesNames = customModulesNames;
            }
            return this;
        }

        public Builder customModulesURIs(List<String> customModulesURIs) {
            if (customModulesURIs != null) {
                config.customModulesURIs = customModulesURIs;
            }
            return this;
        }

        public Builder namedParameters(Map<String, String> namedParameters) {
            if (namedParameters != null) {
                config.namedParameters = namedParameters;
            }
            return this;
        }

        public AppConfig build() {
            return config;
        }

    }
}
