package org.apache.s4.fluent;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class PEZ extends ProcessingElement {

    private Stream<EventA> stream1;
    private Stream<EventB> stream2;

    public PEZ(App app) {
        super(app);
    }

    /**
     * @return the stream1
     */
    Stream<EventA> getStream1() {
        return stream1;
    }

    /**
     * @param stream1
     *            the stream1 to set
     */
    void setStream1(Stream<EventA> stream1) {
        this.stream1 = stream1;
    }

    /**
     * @return the stream2
     */
    Stream<EventB> getStream2() {
        return stream2;
    }

    /**
     * @param stream2
     *            the stream2 to set
     */
    void setStream2(Stream<EventB> stream2) {
        this.stream2 = stream2;
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
