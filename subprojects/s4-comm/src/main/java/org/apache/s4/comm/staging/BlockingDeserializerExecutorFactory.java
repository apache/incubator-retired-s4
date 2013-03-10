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
package org.apache.s4.comm.staging;

import java.util.concurrent.Executor;

import org.apache.s4.comm.DeserializerExecutorFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Executors factory for the deserialization stage that blocks incoming tasks when the work queue is full.
 * 
 */
public class BlockingDeserializerExecutorFactory implements DeserializerExecutorFactory {

    @Named("s4.listener.maxEventsPerDeserializer")
    @Inject(optional = true)
    protected int maxEventsPerDeserializer = 100000;

    @Override
    public Executor create() {
        return new BlockingThreadPoolExecutorService(1, false, "deserializer-%d", maxEventsPerDeserializer, getClass()
                .getClassLoader());
    }

}
