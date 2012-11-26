package org.apache.s4.tools.yarn;

import java.util.List;
import java.util.regex.Pattern;

public class Utils {

    /**
     * The container memory reduction factor as used in: {@link CommonS4YarnArgs#JAVA_MEMORY_ALLOCATION_NOTICE}
     * 
     */
    public static final double CONTAINER_MEMORY_REDUCTION_FACTOR = 0.8;

    /**
     * Extracts Xmx JVM arg by evaluating container memory arg and JVM overriding args
     * 
     * @param containerMemoryArg
     *            The container memory parameter, in MB
     * @param jvmParamsArg
     *            The list of JVM parameters, which may include Xmx parameter
     * @return Memory to reserve as Xmx java parameter, in MB
     */
    static int extractMemoryParam(int containerMemoryArg, List<String> jvmParamsArg) {
        int memory = (int) (containerMemoryArg * CONTAINER_MEMORY_REDUCTION_FACTOR);
        // override maximum heap size
        for (String appVmArg : jvmParamsArg) {
            if (appVmArg.trim().matches("-Xmx\\d+[gGmMkK]")) {
                java.util.regex.Matcher m = Pattern.compile("-Xmx(\\d+)([gGmMkK])").matcher(appVmArg);
                m.matches();
                int memoryInKBytes = Integer.valueOf(m.group(1)) * HeapSizeMultiplier.valueOf(m.group(2)).multiplier
                        / 1000000;
                memory = memoryInKBytes;
                break;
            }
        }
        return memory;
    }

}

enum HeapSizeMultiplier {
    g((int) Math.pow(10, 9)), G((int) Math.pow(10, 9)), m((int) Math.pow(10, 6)), M((int) Math.pow(10, 6)), k(
            (int) Math.pow(10, 3)), K((int) Math.pow(10, 3));
    int multiplier;

    HeapSizeMultiplier(int multiplier) {
        this.multiplier = multiplier;
    }
}
