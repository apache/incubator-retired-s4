package org.apache.s4.comm.topology;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ZNRecord {

    String id;

    public String getId() {
        return id;
    }

    Map<String, String> simpleFields;
    Map<String, List<String>> listFields;
    Map<String, Map<String, String>> mapFields;

    public ZNRecord() {

    }

    public ZNRecord(String id) {
        this.id = id;
        simpleFields = new TreeMap<String, String>();
        mapFields = new TreeMap<String, Map<String, String>>();
        listFields = new TreeMap<String, List<String>>();
    }

    public ZNRecord(ZNRecord that) {
        this(that.id);
        simpleFields.putAll(that.simpleFields);
        mapFields.putAll(that.mapFields);
        listFields.putAll(that.listFields);
    }

    public void setSimpleField(String key, String value) {
        simpleFields.put(key, value);
    }

    public String getSimpleField(String key) {
        return simpleFields.get(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof ZNRecord) {
            ZNRecord that = (ZNRecord) obj;
            return this.id.equals(that.id)
                    && this.simpleFields.equals(that.simpleFields)
                    && this.mapFields.equals(that.mapFields)
                    && this.listFields.equals(that.listFields);
        }
        return false;
    }
}
