package org.apache.s4.core.ft;

import java.util.Map;
import java.util.TimerTask;

import org.apache.s4.core.ProcessingElement;

/**
 * When checkpointing at regular time intervals, this class is used to actually perform the checkpoints. It iterates
 * among all instances of the specified prototype, and checkpoints every eligible instance.
 *
 */
public class CheckpointingTask extends TimerTask {

    ProcessingElement prototype;

    public CheckpointingTask(ProcessingElement prototype) {
        super();
        this.prototype = prototype;
    }

    @Override
    public void run() {
        Map<String, ProcessingElement> peInstances = prototype.getPEInstances();
        for (Map.Entry<String, ProcessingElement> entry : peInstances.entrySet()) {
            synchronized (entry.getValue()) {
                if (entry.getValue().isDirty()) {
                    entry.getValue().checkpoint();
                }
            }
        }
    }
}
