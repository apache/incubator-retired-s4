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

import java.io.InputStream;
import java.net.URI;

/**
 * This interface defines methods to fetch S4R archive files from a URI. Various protocols can be supported in the
 * implementation classes (e.g. file system, HTTP etc...)
 * 
 */
public interface S4RFetcher {

    /**
     * Returns a stream to an S4R archive file
     * 
     * @param uri
     *            S4R archive identifier
     * @return an input stream for accessing the content of the S4R file
     * @throws DeploymentFailedException
     *             when fetching fails
     */
    InputStream fetch(URI uri) throws DeploymentFailedException;

}
