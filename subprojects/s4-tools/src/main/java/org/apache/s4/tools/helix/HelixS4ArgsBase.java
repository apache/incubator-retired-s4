package org.apache.s4.tools.helix;

import org.apache.s4.tools.S4ArgsBase;

import com.beust.jcommander.Parameter;

/**
 * Base class for args for Helix related S4 tools
 * 
 */
public abstract class HelixS4ArgsBase extends S4ArgsBase {

    @Parameter(names = { "-helix" }, description = "Use Helix - required for Helix-related S4 commands", required = true, hidden = true, arity = 0)
    boolean helix;

}
