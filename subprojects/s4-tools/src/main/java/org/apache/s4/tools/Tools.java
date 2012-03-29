package org.apache.s4.tools;

import java.lang.reflect.Method;
import java.util.Arrays;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

public class Tools {

    public static void main(String[] args) {
        try {
            Class<?> toolClass = Class.forName(args[0]);
            Method main = toolClass.getMethod("main", String[].class);
            if (args.length > 1) {
                main.invoke(null, new Object[] { Arrays.copyOfRange(args, 1, args.length) });
            } else {
                main.invoke(null, new Object[] { new String[0] });
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void parseArgs(Object jcArgs, String[] cliArgs) {
        JCommander jc = new JCommander(jcArgs);
        try {
            if (Sets.newHashSet(cliArgs).contains("-help")) {
                Parameters parametersAnnotation = jcArgs.getClass().getAnnotation(Parameters.class);
                jc.addCommand(parametersAnnotation.commandNames()[0], jcArgs);
                jc.usage(parametersAnnotation.commandNames()[0]);
                System.exit(0);
            }
            jc.parse(cliArgs);
        } catch (Exception e) {
            JCommander.getConsole().println("Cannot parse arguments: " + e.getMessage());
            jc.usage();
            System.exit(-1);
        }
    }
}
