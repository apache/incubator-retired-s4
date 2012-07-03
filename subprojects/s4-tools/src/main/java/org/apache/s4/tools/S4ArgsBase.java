package org.apache.s4.tools;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

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
