package org.apache.s4.appmaker;

import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class PEY extends ProcessingElement {

    private Stream<EventB> stream3;

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    /**
     * @return the stream3
     */
    Stream<EventB> getStream3() {
        return stream3;
    }

    /**
     * @param stream3
     *            the stream3 to set
     */
    void setStream3(Stream<EventB> stream3) {
        this.stream3 = stream3;
    }

}
