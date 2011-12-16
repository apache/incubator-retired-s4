package org.apache.s4.fluent;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class PEX extends ProcessingElement {

    private String query;
    private Stream<EventB> someStream;

    public PEX(App app) {
        super(app);
    }

    @Override
    protected void onCreate() {

    }

    @Override
    protected void onRemove() {

    }

    /**
     * @return the keyword
     */
    String getKeyword() {
        return query;
    }

    /**
     * @param query
     *            the keyword to set
     */
    void setKeyword(String query) {
        this.query = query;
    }

    /**
     * @return the someStream
     */
    public Stream<EventB> getSomeStream() {
        return someStream;
    }

    /**
     * @param someStream
     *            the someStream to set
     */
    public void setSomeStream(Stream<EventB> someStream) {
        this.someStream = someStream;
    }

}
