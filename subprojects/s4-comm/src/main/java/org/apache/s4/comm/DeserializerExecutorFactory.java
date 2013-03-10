/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
