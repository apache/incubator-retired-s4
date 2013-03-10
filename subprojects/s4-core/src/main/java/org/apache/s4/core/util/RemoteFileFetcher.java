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
package org.apache.s4.core.util;

import java.io.InputStream;
import java.net.URI;

/**
 * Factory for remote file fetchers depending on the access protocol.
 * 
 */
public class RemoteFileFetcher implements ArchiveFetcher {

    @Override
    public InputStream fetch(URI uri) throws ArchiveFetchException {
        String scheme = uri.getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            return new FileSystemArchiveFetcher().fetch(uri);
        }
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            return new HttpArchiveFetcher().fetch(uri);
        }
        throw new ArchiveFetchException("Unsupported protocol " + scheme);
    }
}
