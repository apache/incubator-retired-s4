package org.apache.s4.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.s4.tools.Deploy.ExecGradle;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

public class Package extends S4ArgsBase {

    public static void main(String[] args) {
        try {
            final PackageArgs packageArgs = new PackageArgs();
            Tools.parseArgs(packageArgs, args);

            List<String> params = new ArrayList<String>();
            // prepare gradle -P parameters, including passed gradle opts
            params.add("appClass=" + packageArgs.appClass);
            params.add("appName=" + packageArgs.appName.get(0));
            ExecGradle.exec(packageArgs.gradleBuildFile, "installS4R", params.toArray(new String[] {}));

            // Explicitly shutdown the JVM since Gradle leaves non-daemon threads running that delay the termination
            System.exit(0);
        } catch (Exception e) {
            LoggerFactory.getLogger(Package.class).error("Cannot deploy app", e);
        }
    }

    @Parameters(commandNames = "package", separators = "=", commandDescription = "Create s4r")
    static class PackageArgs extends S4ArgsBase {

        @Parameter(description = "name of the application", required = true, arity = 1)
        List<String> appName;

        @Parameter(names = { "-b", "-buildFile" }, description = "Path to gradle build file for the S4 application", required = true, converter = FileConverter.class, validateWith = FileExistsValidator.class)
        File gradleBuildFile;

        @Parameter(names = { "-a", "-appClass" }, description = "Full class name of the application class (extending App or AdapterApp)", required = false)
        String appClass = "";

    }
}
