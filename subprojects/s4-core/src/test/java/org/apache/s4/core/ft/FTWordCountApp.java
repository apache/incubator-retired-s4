package org.apache.s4.core.ft;

import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;
import org.apache.s4.wordcount.WordCountApp;

public class FTWordCountApp extends WordCountApp {

    @Override
    protected void onInit() {
        super.onInit();
        getPE("classifierPE").setCheckpointingConfig(
                new CheckpointingConfig.Builder(CheckpointingMode.EVENT_COUNT).frequency(1).build());
        getPE("counterPE").setCheckpointingConfig(
                new CheckpointingConfig.Builder(CheckpointingMode.EVENT_COUNT).frequency(1).build());

    }

}
