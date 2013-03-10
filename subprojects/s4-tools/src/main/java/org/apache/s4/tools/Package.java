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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.s4.tools.Deploy.ExecGradle;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

/**
 * Packages an S4 application and dependencies into an S4R file, i.e. a jar file with some S4 specific meta information.
 */
public class Package extends S4ArgsBase {

    public static void main(String[] args) {
        try {
            final PackageArgs packageArgs = new PackageArgs();
            Tools.parseArgs(packageArgs, args);

            List<String> params = new ArrayList<String>();
            // prepare gradle -P parameters, including passed gradle opts
            params.add("appClass=" + packageArgs.appClass);
            params.add("appName=" + packageArgs.appName.get(0));
            ExecGradle.exec(packageArgs.gradleBuildFile, "s4r", params.toArray(new String[] {}), packageArgs.debug);

        } catch (Exception e) {
            LoggerFactory.getLogger(Package.class).error("Cannot deploy app", e);
        }
        // Explicitly shutdown the JVM since Gradle leaves non-daemon threads running that delay the termination
        System.exit(0);
    }

    @Parameters(commandNames = "package", separators = "=", commandDescription = "Create s4r")
    static class PackageArgs extends S4ArgsBase {

        @Parameter(description = "name of the application", required = true, arity = 1)
        List<String> appName;

        @Parameter(names = { "-b", "-buildFile" }, description = "Path to gradle build file for the S4 application", required = true, converter = FileConverter.class, validateWith = FileExistsValidator.class)
        File gradleBuildFile;

        @Parameter(names = { "-a", "-appClass" }, description = "Full class name of the application class (extending App or AdapterApp)", required = true)
        String appClass = "";

        @Parameter(names = "-debug", description = "Display debug information from the build system", arity = 0)
        boolean debug = false;

    }
}
