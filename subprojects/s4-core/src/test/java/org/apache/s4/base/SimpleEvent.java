package org.apache.s4.base;

public class SimpleEvent extends Event {

    private String name;

    public SimpleEvent(String name) {
        super();
        this.name = name;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
}
