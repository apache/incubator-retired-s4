package org.apache.s4.core.staging;

import java.util.concurrent.ExecutorService;

/**
 * Defines a factory that creates executors for the stage responsible for the serialization of events and delegation to
 * emitters in the communication layer.
 * 
 */
public interface SenderExecutorServiceFactory {

    ExecutorService create();
}
