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
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

/**
 * Creates a template S4 project
 */
public class CreateApp extends S4ArgsBase {

    static Logger logger = LoggerFactory.getLogger(CreateApp.class);

    public static void main(String[] args) {

        final CreateAppArgs appArgs = new CreateAppArgs();
        Tools.parseArgs(appArgs, args);

        if (new File(appArgs.getAppDir() + "/" + appArgs.appName.get(0)).exists()) {
            System.err.println("There is already a directory called " + appArgs.appName.get(0) + " in the "
                    + appArgs.getAppDir()
                    + " directory. Please specify another name for your project or specify another parent directory");
            System.exit(1);
        }
        // create project structure
        try {
            createDir(appArgs, "/src/main/java");
            createDir(appArgs, "/src/main/resources");
            createDir(appArgs, "/src/main/java/hello");

            // copy gradlew script (redirecting to s4 gradlew)
            File gradlewTempFile = File.createTempFile("gradlew", "tmp");
            gradlewTempFile.deleteOnExit();
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/gradlew")), gradlewTempFile);
            String gradlewScriptContent = Files.readLines(gradlewTempFile, Charsets.UTF_8, new PathsReplacer(appArgs));
            Files.write(gradlewScriptContent, gradlewTempFile, Charsets.UTF_8);
            Files.copy(gradlewTempFile, new File(appArgs.getAppDir() + "/gradlew"));
            new File(appArgs.getAppDir() + "/gradlew").setExecutable(true);

            // copy build file contents
            String buildFileContents = Resources.toString(Resources.getResource("templates/build.gradle"),
                    Charsets.UTF_8);
            buildFileContents = buildFileContents.replace("<s4_install_dir>",
                    "'" + new File(appArgs.s4ScriptPath).getParent() + "'");
            Files.write(buildFileContents, new File(appArgs.getAppDir() + "/build.gradle"), Charsets.UTF_8);

            // copy lib
            FileUtils.copyDirectory(new File(new File(appArgs.s4ScriptPath).getParentFile(), "lib"),
                    new File(appArgs.getAppDir() + "/lib"));

            // update app settings
            String settingsFileContents = Resources.toString(Resources.getResource("templates/settings.gradle"),
                    Charsets.UTF_8);
            settingsFileContents = settingsFileContents.replaceFirst("rootProject.name=<project-name>",
                    "rootProject.name=\"" + appArgs.appName.get(0) + "\"");
            Files.write(settingsFileContents, new File(appArgs.getAppDir() + "/settings.gradle"), Charsets.UTF_8);
            // copy hello app files
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/HelloPE.java.txt")), new File(
                    appArgs.getAppDir() + "/src/main/java/hello/HelloPE.java"));
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/HelloApp.java.txt")),
                    new File(appArgs.getAppDir() + "/src/main/java/hello/HelloApp.java"));
            // copy hello app adapter
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/HelloInputAdapter.java.txt")),
                    new File(appArgs.getAppDir() + "/src/main/java/hello/HelloInputAdapter.java"));

            File s4TmpFile = File.createTempFile("s4Script", "template");
            s4TmpFile.deleteOnExit();
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/s4")), s4TmpFile);

            // create s4
            String preparedS4Script = Files.readLines(s4TmpFile, Charsets.UTF_8, new PathsReplacer(appArgs));

            File s4Script = new File(appArgs.getAppDir() + "/s4");
            Files.write(preparedS4Script, s4Script, Charsets.UTF_8);
            s4Script.setExecutable(true);

            File readmeTmpFile = File.createTempFile("newApp", "README");
            readmeTmpFile.deleteOnExit();
            Files.copy(Resources.newInputStreamSupplier(Resources.getResource("templates/newApp.README")),
                    readmeTmpFile);
            // display contents from readme
            Files.readLines(readmeTmpFile, Charsets.UTF_8, new LineProcessor<Boolean>() {

                @Override
                public boolean processLine(String line) throws IOException {
                    if (!line.startsWith("#")) {
                        System.out.println(line.replace("<appDir>", appArgs.getAppDir()));
                    }
                    return true;
                }

                @Override
                public Boolean getResult() {
                    return true;
                }

            });
        } catch (Exception e) {
            logger.error("Could not create project due to [{}]. Please check your configuration.", e.getMessage());
        }
    }

    private static void createDir(CreateAppArgs appArgs, String dirName) throws Exception {
        String filePath = appArgs.getAppDir() + dirName;
        if (!new File(filePath).mkdirs()) {
            throw new Exception("Cannot create directory [" + filePath + "]");
        }
    }

    private static final class PathsReplacer implements LineProcessor<String> {
        private final CreateAppArgs appArgs;
        StringBuilder sb = new StringBuilder();

        private PathsReplacer(CreateAppArgs appArgs) {
            this.appArgs = appArgs;
        }

        @Override
        public boolean processLine(String line) throws IOException {
            sb.append(line.replace("<s4_script_path>", appArgs.s4ScriptPath).replace("<s4_install_dir>",
                    new File(appArgs.s4ScriptPath).getParent())
                    + "\n");
            return true;
        }

        @Override
        public String getResult() {
            return sb.toString();
        }
    }

    @Parameters(commandNames = "newApp", separators = "=", commandDescription = "Create new application skeleton")
    static class CreateAppArgs extends S4ArgsBase {

        @Parameter(description = "name of the application", required = true, arity = 1)
        List<String> appName;

        @Parameter(names = "-parentDir", description = "parent directory of the application")
        String parentDir = System.getProperty("user.dir");

        public String getAppDir() {
            return parentDir + "/" + appName.get(0);
        }

    }
}
