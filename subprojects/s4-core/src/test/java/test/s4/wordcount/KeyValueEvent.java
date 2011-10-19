package test.s4.wordcount;

import test.s4.wordcount.StringEvent;

public class KeyValueEvent extends StringEvent {

    String key;
    String value;

    public KeyValueEvent(String keyValue) {
        key = keyValue.split(";")[0];
        value = keyValue.split(";")[1];
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

}
