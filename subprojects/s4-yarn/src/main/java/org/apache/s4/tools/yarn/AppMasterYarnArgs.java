package org.apache.s4.tools.yarn;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class AppMasterYarnArgs extends CommonS4YarnArgs {

    @Parameter(names = "-app_attempt_id", description = "App Attempt ID. Not to be used unless for testing purposes", required = false)
    String appAttemptId;

}
