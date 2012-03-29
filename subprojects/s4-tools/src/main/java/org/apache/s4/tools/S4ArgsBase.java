package org.apache.s4.tools;

import com.beust.jcommander.Parameter;

public abstract class S4ArgsBase {

    @Parameter(names = "-help", description = "usage")
    boolean help = false;

}
