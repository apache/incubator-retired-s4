package org.apache.s4.fluent;

import org.apache.s4.core.App;

public class FluentApp extends App {

    final private AppMaker appMaker;

    public FluentApp(AppMaker appMaker) {
        this.appMaker = appMaker;
    }

    @Override
    protected void onStart() {
        appMaker.onStart();
    }

    @Override
    protected void onInit() {
        appMaker.onInit();
    }

    @Override
    protected void onClose() {
        appMaker.onClose();
    }
}
