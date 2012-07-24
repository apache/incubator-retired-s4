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

/**
 * <p>This package contains classes for providing some fault tolerance
 *     to S4 PEs.</p>
 * <p>The current approach is based on <b>checkpointing</b>.</p>
 *  <p>Checkpoints are taken periodically (configurable by time or
 *  frequency of application events), and when restarting an S4 node,
 *  saved checkpoints are automatically and lazily restored.</p>
 *  <p><b>Lazy restoration</b> is triggered by an application event to a PE
 *  that has not yet been restored.</p>
 *  <p>Checkpoints are stored in storage backends. Storage backends may
 *  implement eager techniques to prefetch checkpoint data to be
 *  recovered. Storage backends can be implemented for various kinds of systems,
 *  and must implement the {@link org.apache.s4.core.ft.StateStorage} interface.
 *  They are pluggable throught the module system.
 *  <p>
 *  The application programmer must take care of marking as <b>transient</b>
 *  the fields that do not have to be persisted (or cannot be persisted).
 *  <p>Storage backends are pluggable and we provide some default
 *  implementations in this package</p>
 */
package org.apache.s4.core.ft;

