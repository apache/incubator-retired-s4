package org.apache.s4.tools;

import java.lang.reflect.Method;
import java.util.Arrays;

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
}
