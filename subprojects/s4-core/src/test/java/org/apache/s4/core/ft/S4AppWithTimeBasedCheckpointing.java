package org.apache.s4.core.ft;

import java.util.concurrent.TimeUnit;

import org.apache.s4.core.App;
import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;

public class S4AppWithTimeBasedCheckpointing extends App {

    @Override
    protected void onStart() {
    }

    @Override
    protected void onInit() {
        StatefulTestPE pe = createPE(StatefulTestPE.class);
        pe.setSingleton(true);
        // checkpoints (if applicable) every 1 ms!
        pe.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.TIME).frequency(1)
                .timeUnit(TimeUnit.MILLISECONDS).build());
        createInputStream("inputStream", pe);
    }

    @Override
    protected void onClose() {
    }

}
