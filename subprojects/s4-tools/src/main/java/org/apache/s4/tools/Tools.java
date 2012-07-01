package org.apache.s4.tools;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.s4.core.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

public class Tools {

    static Logger logger = LoggerFactory.getLogger(Tools.class);

    enum Task {
        deploy(Deploy.class), node(Main.class), zkServer(ZKServer.class), newCluster(DefineCluster.class), adapter(null), newApp(
                CreateApp.class), s4r(Package.class);

        Class<?> target;

        Task(Class<?> target) {
            this.target = target;
        }

        public void dispatch(String[] args) {
            try {
                Method main = target.getMethod("main", String[].class);
                main.invoke(null, new Object[] { args });
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Cannot dispatch to task [{}]: wrong arguments [{}]", this.name(), Arrays.toString(args));
            }
        }

    }

    public static void main(String[] args) {

        // configure log4j for Zookeeper
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        org.apache.log4j.Logger.getLogger("org.I0Itec").setLevel(Level.ERROR);

        if (!(args.length > 1)) {
            List<String> taskNames = getTaskNames();
            System.err.println("please specify a task name and proper arguments. Available tasks are: "
                    + Arrays.toString(taskNames.toArray(new String[] {})));
            System.exit(1);
        }

        // then we just pass all arguments without the task name
        Task task = null;
        try {
            // first argument is -s4ScriptPath=x
            task = Task.valueOf(args[1]);
        } catch (IllegalArgumentException e) {
            System.err.println("please specify a task name and proper arguments. Available tasks are: "
                    + Arrays.toString(getTaskNames().toArray(new String[] {})));
            System.exit(1);
        }
        List<String> taskArgs = new ArrayList<String>();
        if (!task.name().equals("node")) {
            taskArgs.add(args[0]); // s4 script (only for s4-tools project classes)
        }
        if (args.length > 1) {
            taskArgs.addAll(Arrays.asList(Arrays.copyOfRange(args, 2, args.length)));
        }
        task.dispatch(taskArgs.toArray(new String[] {}));

    }

    private static List<String> getTaskNames() {
        Task[] tasks = Task.values();
        List<String> taskNames = new ArrayList<String>();
        for (Task task : tasks) {
            taskNames.add(task.name());
        }
        return taskNames;
    }

    public static JCommander parseArgs(Object jcArgs, String[] cliArgs) {
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
            JCommander.getConsole().println("Cannot parse arguments: " + e.getClass() + " -> " + e.getMessage());
            jc.usage();
            System.exit(1);
        }
        return jc;
    }

    @Parameters
    static class ToolsArgs {
        @Parameter(description = "Name of the task", required = true)
        String taskName;

    }
}
