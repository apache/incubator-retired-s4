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

package org.apache.s4.deploy;

/**
 * 
 * <p>
 * Indicates that an application failed to be deployed to an S4 node.
 * </p>
 * <p>
 * Thrown during detection, fetching, loading or startup of applications deployed from a repository.
 * </p>
 */
public class DeploymentFailedException extends Exception {

    public DeploymentFailedException(String message, Throwable e) {
        super(message, e);
    }

    public DeploymentFailedException(String message) {
        super(message);
    }

}
