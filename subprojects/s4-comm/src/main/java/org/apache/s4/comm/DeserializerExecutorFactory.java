package org.apache.s4.comm;

import java.util.concurrent.Executor;

/**
 * Factory for deserializer executors used in listener pipelines.
 * <p>
 * Deserialization is a relatively costly operation, depending on the event type. This operation can be parallelized,
 * and we provide channel workers an executor for that purpose.
 * <p>
 * There are many possible implementations, that may consider various factors, in particular:
 * <ul>
 * <li>parallelism
 * <li>memory usage
 * <li>sharing threadpool among channel workers
 * 
 * 
 */
public interface DeserializerExecutorFactory {

    Executor create();

}
