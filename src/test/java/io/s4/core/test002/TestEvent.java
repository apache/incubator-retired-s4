package io.s4.core.test002;

import io.s4.core.Event;

public class TestEvent extends Event {
    private String key;
    private long value;

    TestEvent(String key, long count) {
        this.key = key;
        this.value = count;
    }
    
    public TestEvent() {}

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @return the count
     */
    public long getCount() {
        return value;
    }
}
