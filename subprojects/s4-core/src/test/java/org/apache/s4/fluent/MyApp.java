package org.apache.s4.fluent;

import java.util.concurrent.TimeUnit;

public class MyApp extends AppMaker {

    @Override
    protected void configure() {

        PEMaker pez, pey, pex;

        /* Configure processing element pez. */
        pez = addPE(PEZ.class);
        pez.addTrigger().fireOn(EventA.class).ifInterval(5, TimeUnit.SECONDS);
        pez.addCache().ofSize(1000).withDuration(3, TimeUnit.HOURS);

        /* Configure processing element pey. */
        pey = addPE(PEY.class).with("duration", 4).with("height", 99);
        pey.addTimer().withDuration(2, TimeUnit.MINUTES);

        /* Configure processing element pex. */
        pex = addPE(PEX.class).with("query", "money");
        pex.addCache().ofSize(100).withDuration(1, TimeUnit.MINUTES);

        /* Construct the graph. */
        pey.emit(EventA.class).onKey(new DurationKeyFinder()).to(pez);
        pex.emit(EventB.class).onKey(new QueryKeyFinder()).to(pez);
        pex.emit(EventB.class).onKey(new QueryKeyFinder()).to(pey).to(pez);
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }
}
