package org.apache.s4.edsl;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class PEY extends ProcessingElement {

    private Stream<EventA>[] stream3;
    @SuppressWarnings("unused")
    private Stream<EventA>[] heightpez;

    private int height;
    private long duration;

    public PEY(App app) {
        super(app);
    }

    @Override
    public void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onRemove() {
        // TODO Auto-generated method stub

    }

    /**
     * @return the stream3
     */
    Stream<EventA>[] getStream3() {
        return stream3;
    }

    /**
     * @param stream3
     *            the stream3 to set
     */
    void setStream3(Stream<EventA>[] stream3) {
        this.stream3 = stream3;
    }

    /**
     * @return the height
     */
    int getHeight() {
        return height;
    }

    /**
     * @param height
     *            the height to set
     */
    void setHeight(int height) {
        this.height = height;
    }

    /**
     * @return the duration
     */
    long getDuration() {
        return duration;
    }

    /**
     * @param duration
     *            the duration to set
     */
    void setDuration(long duration) {
        this.duration = duration;
    }

}
