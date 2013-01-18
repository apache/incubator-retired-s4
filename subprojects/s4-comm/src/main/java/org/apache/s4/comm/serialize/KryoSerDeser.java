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

package org.apache.s4.comm.serialize;

import java.nio.ByteBuffer;

import org.apache.s4.base.SerializerDeserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * Serializer/deserializer based on <a href="http://code.google.com/p/kryo/">kryo 2</a>
 * 
 */
public class KryoSerDeser implements SerializerDeserializer {

    private ThreadLocal<Kryo> kryoThreadLocal;
    private ThreadLocal<Output> outputThreadLocal;

    private int initialBufferSize = 2048;
    private int maxBufferSize = 256 * 1024;

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * 
     * @param classLoader
     *            classloader able to handle classes to serialize/deserialize. For instance, application-level events
     *            can only be handled by the application classloader.
     */
    @Inject
    public KryoSerDeser(@Assisted final ClassLoader classLoader) {
        kryoThreadLocal = new ThreadLocal<Kryo>() {

            @Override
            protected Kryo initialValue() {
                Kryo kryo = new Kryo();
                kryo.setClassLoader(classLoader);
                kryo.setRegistrationRequired(false);
                return kryo;
            }
        };

        outputThreadLocal = new ThreadLocal<Output>() {
            @Override
            protected Output initialValue() {
                Output output = new Output(initialBufferSize, maxBufferSize);
                return output;
            }
        };

    }

    @Override
    public Object deserialize(ByteBuffer rawMessage) {
        Input input = new Input(rawMessage.array());
        try {
            return kryoThreadLocal.get().readClassAndObject(input);
        } finally {
            input.close();
        }
    }

    @SuppressWarnings("resource")
    @Override
    public ByteBuffer serialize(Object message) {
        Output output = outputThreadLocal.get();
        try {
            kryoThreadLocal.get().writeClassAndObject(output, message);
            return ByteBuffer.wrap(output.toBytes());
        } finally {
            output.clear();
        }
    }
}
