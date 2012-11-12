package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

/**
 * Defines an executor factory for the stage responsible for sending events to remote logical clusters.
 * 
 */
public interface RemoteSendersExecutorServiceFactory {

    ExecutorService create();
}
