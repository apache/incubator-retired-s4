package org.apache.s4.core.ft;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a checkpoint fetching operation.
 *
 */
public class FetchTask implements Callable<byte[]> {

    private static Logger logger = LoggerFactory.getLogger(FetchTask.class);

    StateStorage stateStorage;
    CheckpointId checkpointId;

    public FetchTask(StateStorage stateStorage, CheckpointId checkpointId) {
        super();
        this.stateStorage = stateStorage;
        this.checkpointId = checkpointId;
    }

    @Override
    public byte[] call() throws Exception {
        try {
            byte[] result = stateStorage.fetchState(checkpointId);
            return result;
        } catch (Exception e) {
            logger.error("Cannot fetch checkpoint data for {}", checkpointId, e);
            throw e;
        }
    }

}
