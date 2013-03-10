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

package org.apache.s4.base.util;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A classloader that fetches and loads classes and resources from :
 * <ul>
 * <li>Application classes in an S4R archive</li>
 * <li>Application dependencies from an S4R archive</li>
 * <li>Classes dynamically generated (proxies)
 * </ul>
 */
public class S4RLoader extends URLClassLoader {

    public S4RLoader(URL[] urls) {
        super(urls);
    }

    public Class<?> loadGeneratedClass(String name, byte[] bytes) {
        Class<?> clazz = findLoadedClass(name);
        if (clazz == null) {
            return defineClass(name, bytes, 0, bytes.length);
        } else {
            return clazz;
        }
    }

}
