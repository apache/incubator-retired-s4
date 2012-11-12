package org.apache.s4.comm;

import java.util.concurrent.Executor;

/**
 * Factory for deserializer executors used in listener pipelines.
 * 
 */
public interface DeserializerExecutorFactory {

    Executor create();

}
