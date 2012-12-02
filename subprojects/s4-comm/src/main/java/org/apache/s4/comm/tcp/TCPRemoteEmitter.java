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

package org.apache.s4.comm.tcp;

import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.comm.topology.Cluster;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;

/**
 * Emitter to remote subclusters.
 * 
 */
public class TCPRemoteEmitter extends TCPEmitter implements RemoteEmitter {

    /**
     * Sends to remote subclusters. This is dynamically created, through an injected factory, when new subclusters are
     * discovered (as remote streams outputs).
     * <p>
     * See {@link TCPEmitter} for more information about the implementation.
     */
    @Inject
    public TCPRemoteEmitter(@Assisted Cluster topology, @Named("s4.comm.timeout") int timeout,
            @Named("s4.emitter.maxPendingWrites") int maxPendingWrites) throws InterruptedException {
        super(topology, timeout, maxPendingWrites);
    }

}
