package org.apache.s4.tools;

import com.beust.jcommander.Parameter;

public abstract class S4ArgsBase {

    @Parameter(names = "-help", description = "usage")
    boolean help = false;

    @Parameter(names = "-s4ScriptPath", description = "path of the S4 script", hidden = true, required = true)
    String s4ScriptPath;

}
