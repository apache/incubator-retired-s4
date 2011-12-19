package org.apache.s4.base;

public class SimpleEvent extends Event {

    private String name;
    private long numGrapes;

    public SimpleEvent(String name, long numGrapes) {
        super();
        this.name = name;
        this.numGrapes = numGrapes;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public long getNumGrapes() {
        return numGrapes;
    }
}
