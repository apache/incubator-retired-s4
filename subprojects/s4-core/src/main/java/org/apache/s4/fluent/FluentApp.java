package org.apache.s4.fluent;

import org.apache.s4.core.App;

import com.google.inject.Inject;

/**
 * The Fluent API uses this class to construct apps automatically. Users should not have to use this class directly.
 * 
 */
public class FluentApp extends App {

    final private AppMaker appMaker;

    @Inject
    public FluentApp(AppMaker appMaker) {
        super();
        this.appMaker = appMaker;
        appMaker.setApp(this);
    }

    @Override
    protected void onStart() {
        appMaker.start();
    }

    @Override
    protected void onInit() {
        appMaker.configure();
        appMaker.make();
    }

    @Override
    protected void onClose() {
        appMaker.close();
    }

    public void start() {
        super.start();
    }

    public void init() {
        super.init();
    }

    public void close() {
        super.close();
    }
}
