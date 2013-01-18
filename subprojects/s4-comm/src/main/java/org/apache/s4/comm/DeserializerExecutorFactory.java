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
 * <li>memory usage (directly measured, or inferred from the number of buffered events)
 * <li>sharing threadpool among channel workers
 * </ul>
 * <p>
 * When related thresholds are reached, deserializer executors may:
 * <ul>
 * <li>block: this indirectly blocks the reception of messages for this channel, applying upstream backpressure.
 * <li>drop messages: a form of load shedding
 * 
 * 
 */
public interface DeserializerExecutorFactory {

    Executor create();

}
