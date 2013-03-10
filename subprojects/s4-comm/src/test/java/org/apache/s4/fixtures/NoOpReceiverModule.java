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
package org.apache.s4.fixtures;

import org.apache.s4.base.Receiver;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Avoids delegating message processing to the application layer.
 * 
 */
public class NoOpReceiverModule extends AbstractModule {

    @Provides
    public SerializerDeserializer provideSerializerDeserializer(SerializerDeserializerFactory serDeserFactory) {
        // we use the current classloader here, no app class to serialize
        return serDeserFactory.createSerializerDeserializer(getClass().getClassLoader());
    }

    @Override
    protected void configure() {
        bind(Receiver.class).to(NoOpReceiver.class);
    }

}
