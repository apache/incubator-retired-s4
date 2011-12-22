package org.apache.s4.fixtures;

import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.Main;
import org.apache.s4.fixtures.CommTestUtils;

/**
 * Contains static methods that can be used in tests for things such as: - files utilities: strings <-> files
 * conversion, directory recursive delete etc... - starting local instances for zookeeper and bookkeeper - distributed
 * latches through zookeeper - etc...
 * 
 */
public class CoreTestUtils extends CommTestUtils {

    public static Process forkS4App(Class<?> moduleClass, Class<?> appClass) throws IOException, InterruptedException {
        return forkProcess(App.class.getName(), moduleClass.getName(), appClass.getName());
    }

    public static Process forkS4Node() throws IOException, InterruptedException {
        return forkProcess(Main.class.getName(), new String[] {});
    }
}
