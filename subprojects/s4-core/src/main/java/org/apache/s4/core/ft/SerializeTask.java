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

package org.apache.s4.core.ft;

import java.util.concurrent.Callable;

import org.apache.s4.core.ProcessingElement;

/**
 * Encaspulate a PE serialization operation. This operation locks the PE instance in order to avoid any inconsistent
 * serialized state. If serialization is successful, the PE is marked as "not dirty".
 * 
 */
public class SerializeTask implements Callable<byte[]> {

    ProcessingElement pe;

    public SerializeTask(ProcessingElement pe) {
        super();
        this.pe = pe;
    }

    @Override
    public byte[] call() throws Exception {
        synchronized (pe) {
            byte[] state = pe.serializeState();
            pe.clearDirty();
            return state;
        }
    }
}
