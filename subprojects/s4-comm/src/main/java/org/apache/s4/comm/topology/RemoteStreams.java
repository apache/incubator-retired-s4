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
package org.apache.s4.comm.topology;

import java.util.Set;

/**
 * <p>
 * Monitors streams available in the S4 cluster.
 * </p>
 * <p>
 * Maintains a data structure reflecting the currently published streams with their consumers and publishers.
 * </p>
 * <p>
 * Provides methods to publish producers and consumers of streams
 * </p>
 * 
 */

public interface RemoteStreams {

    /**
     * Lists consumers of a given stream
     */
    public abstract Set<StreamConsumer> getConsumers(String streamName);

    /**
     * Publishes availability of an output stream
     * 
     * @param clusterName
     *            originating cluster
     * @param streamName
     *            name of stream
     */
    public abstract void addOutputStream(String clusterName, String streamName);

    /**
     * Publishes interest in a stream, by a given cluster
     * 
     * @param clusterName
     * @param streamName
     */
    public abstract void addInputStream(String clusterName, String streamName);

}
