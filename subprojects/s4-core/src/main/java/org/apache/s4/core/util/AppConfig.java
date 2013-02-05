package org.apache.s4.core.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.s4.comm.topology.ZNRecord;

import com.beust.jcommander.internal.Maps;

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

    public boolean isValid() {
        return (appClassName != null || appURI != null) && appName != null;
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

    public Map<String, String> asMap() {
        Map<String, String> result = Maps.newHashMap();
        result.put(APP_NAME, appName);
        result.put(APP_URI, appURI);
        StringBuilder sb = new StringBuilder();
        for (String customModuleName : customModulesNames) {
            sb.append(customModuleName + ",");
        }
        result.put(MODULES_CLASSES, sb.toString());
        sb = new StringBuilder();
        for (String customModulesURI : customModulesURIs) {
            sb.append(customModulesURI + ",");
        }
        result.put(MODULES_URIS, sb.toString());
        result.put(NAMED_PARAMETERS, getNamedParametersAsString());
        return result;
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
