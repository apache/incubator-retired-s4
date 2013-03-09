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

package org.apache.s4.tools;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * Common parameters for S4 commands
 */
public abstract class S4ArgsBase {

    @Parameter(names = "-help", description = "usage")
    boolean help = false;

    @Parameter(names = "-s4ScriptPath", description = "path of the S4 script", hidden = true, required = false)
    String s4ScriptPath;

    @Parameter(names = "-gradleOpts", variableArity = true, description = "gradle system properties (as in GRADLE_OPTS environment properties) passed to gradle scripts", required = false, converter = GradleOptsConverter.class)
    List<String> gradleOpts = new ArrayList<String>();

    // This removes automatically the -D of each gradle opt jvm parameter, if present
    public class GradleOptsConverter implements IStringConverter<String> {

        @Override
        public String convert(String value) {
            if (value.startsWith("-D")) {
                return value.substring("-D".length());
            } else {
                return value;
            }
        }

    }
}
