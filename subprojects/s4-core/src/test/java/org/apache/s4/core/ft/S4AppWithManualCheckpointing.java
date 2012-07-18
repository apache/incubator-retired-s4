package org.apache.s4.core.ft;

import org.apache.s4.core.App;

/**
 *
 *
 */
public class S4AppWithManualCheckpointing extends App {

    @Override
    protected void onStart() {

    }

    @Override
    protected void onInit() {
        StatefulTestPE pe = createPE(StatefulTestPE.class);
        pe.setSingleton(true);
        createInputStream("inputStream", pe);
    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

}
