package org.apache.s4.core.ft;

import org.apache.s4.core.App;
import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;

public class S4AppWithCountBasedCheckpointing extends App {

    @Override
    protected void onStart() {
    }

    @Override
    protected void onInit() {
        StatefulTestPE pe = createPE(StatefulTestPE.class);
        pe.setSingleton(true);
        pe.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.EVENT_COUNT).frequency(1).build());
        createInputStream("inputStream", pe);
    }

    @Override
    protected void onClose() {
    }

}
