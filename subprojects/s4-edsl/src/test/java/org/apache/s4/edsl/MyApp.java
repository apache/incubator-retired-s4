package org.apache.s4.edsl;

import java.util.concurrent.TimeUnit;

public class MyApp extends BuilderS4DSL {

    @Override
    public void onInit() {

        pe("PEZ").type(PEZ.class).fireOn(EventA.class).afterInterval(5, TimeUnit.SECONDS).cache().size(1000)
                .expires(3, TimeUnit.HOURS).emit(EventB.class).to("PEX").

                pe("PEY").type(PEY.class).prop("duration", "4").prop("height", "99").timer()
                .withPeriod(2, TimeUnit.MINUTES).emit(EventA.class).onField("stream3")
                .withKeyFinder(DurationKeyFinder.class).to("PEZ").emit(EventA.class).onField("heightpez")
                .withKeyFinder(HeightKeyFinder.class).to("PEZ").

                pe("PEX").type(PEX.class).prop("query", "money").cache().size(100).expires(1, TimeUnit.MINUTES)
                .asSingleton().emit(EventB.class).withKeyFinder(QueryKeyFinder.class).to("PEY", "PEZ").

                build();
    }

    // Make hooks public for testing. Normally this is handled by the container.
    public void init() {
        super.init();
    }

    public void start() {
        super.start();
    }

    public void close() {
        super.close();
    }

}
