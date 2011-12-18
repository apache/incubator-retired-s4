package org.apache.s4.fluent;

import java.util.concurrent.TimeUnit;

public class MyApp extends AppMaker {

    @Override
    public void configure() {

        PEMaker pez, pey, pex;

        /* Configure processing element pez. */
        pez = addPE(PEZ.class);
        pez.addTrigger().fireOn(EventA.class).ifInterval(5, TimeUnit.SECONDS);
        pez.addCache().ofSize(1000).withDuration(3, TimeUnit.HOURS);

        /* Configure processing element pey. */
        pey = addPE(PEY.class).with("duration", 4).with("height", 99);
        pey.addTimer().withDuration(2, TimeUnit.MINUTES);

        /* Configure processing element pex. */
        pex = addPE(PEX.class).with("query", "money").asSingleton();
        pex.addCache().ofSize(100).withDuration(1, TimeUnit.MINUTES);

        /* Construct the graph. */
        pey.emit(EventA.class).withField("stream3").onKey(new DurationKeyFinder()).to(pez);
        pey.emit(EventA.class).withField("heightpez").onKey(new HeightKeyFinder()).to(pez);
        pez.emit(EventB.class).to(pex);
        pex.emit(EventB.class).onKey(new QueryKeyFinder()).to(pey).to(pez);
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
