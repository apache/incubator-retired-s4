/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.core.gen;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.F_APPEND;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.INSTANCEOF;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_6;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.s4.base.Event;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.core.ProcessingElement;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * This class generates a proxy to enable dispatching of events to methods of processing elements based on the runtime
 * type of the event.
 * 
 * <p>
 * When an event is transferred to a processing element, the generated proxy finds the corresponding
 * <code>onEvent</code> method with the event type argument matching the current parameter and calls this method.
 * </p>
 * <p>
 * If there is no exact match, the closest type in the hierarchy of events is used.
 * </p>
 * <p>
 * If there is still no match, an error statement is logged and the event is ignored (not processed).
 * </p>
 * 
 */
public class OverloadDispatcherGenerator {
    private final List<Hierarchy> inputEventHierarchies = new ArrayList<Hierarchy>();
    private final List<Hierarchy> outputEventHierarchies = new ArrayList<Hierarchy>();
    private Class<?> targetClass;
    private static final boolean DUMP = false;

    public OverloadDispatcherGenerator() {
    }

    public OverloadDispatcherGenerator(Class<?> targetClass) {
        this.targetClass = targetClass;

        for (Method method : targetClass.getMethods()) {
            if (method.getName().equals("onEvent") && method.getReturnType().equals(Void.TYPE)) {
                inputEventHierarchies.add(new Hierarchy(method.getParameterTypes()[0]));
            } else if (method.getName().equals("onTrigger") && method.getReturnType().equals(Void.TYPE)) {
                outputEventHierarchies.add(new Hierarchy(method.getParameterTypes()[0]));
            }
        }
        // order by most specialized types
        Collections.sort(inputEventHierarchies);
        Collections.sort(outputEventHierarchies);
    }

    public Class<?> generate() {
        Random rand = new Random(System.currentTimeMillis());
        String dispatcherClassName = "OverloadDispatcher" + (Math.abs(rand.nextInt() % 3256));

        // class headers
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        // CheckClassAdapter cw = new CheckClassAdapter(cw1);
        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, dispatcherClassName, null, Type.getInternalName(Object.class),
                new String[] { Type.getInternalName(OverloadDispatcher.class) });

        // constructor
        MethodVisitor mv1 = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv1.visitCode();
        Label l0 = new Label();
        mv1.visitLabel(l0);
        mv1.visitVarInsn(ALOAD, 0);
        mv1.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
        Label l1 = new Label();
        mv1.visitLabel(l1);
        mv1.visitInsn(RETURN);
        Label l2 = new Label();
        mv1.visitLabel(l2);
        mv1.visitLocalVariable("this", "Lio/s4/core/" + dispatcherClassName + ";", null, l0, l2, 0);
        mv1.visitMaxs(1, 1);

        mv1.visitEnd();

        // dispatch input events method
        generateEventDispatchMethod(cw, "dispatchEvent", inputEventHierarchies, "onEvent");
        // dispatch output events method
        generateEventDispatchMethod(cw, "dispatchTrigger", outputEventHierarchies, "onTrigger");

        cw.visitEnd();

        if (DUMP) {
            try {
                LoggerFactory.getLogger(getClass()).debug(
                        "Dumping generated overload dispatcher class for PE of class [" + targetClass + "]");
                Files.write(cw.toByteArray(), new File(System.getProperty("java.io.tmpdir") + "/" + dispatcherClassName
                        + ".class"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new OverloadDispatcherClassLoader(targetClass.getClassLoader()).loadClassFromBytes(dispatcherClassName,
                cw.toByteArray());

    }

    private void generateEventDispatchMethod(ClassWriter cw, String dispatchMethodName,
            List<Hierarchy> eventHierarchies, String processEventMethodName) {
        MethodVisitor mv2 = cw.visitMethod(ACC_PUBLIC, dispatchMethodName, "("
                + Type.getType(ProcessingElement.class).getDescriptor() + Type.getType(Event.class).getDescriptor()
                + ")V", null, null);
        mv2.visitCode();
        Label l3 = new Label();
        mv2.visitLabel(l3);
        mv2.visitVarInsn(ALOAD, 1);
        mv2.visitTypeInsn(CHECKCAST, Type.getInternalName(targetClass));
        mv2.visitVarInsn(ASTORE, 3);
        boolean first = true;
        Label aroundLabel = new Label();
        for (Hierarchy hierarchy : eventHierarchies) {
            if (first) {
                Label l4 = new Label();
                mv2.visitLabel(l4);
            }
            mv2.visitVarInsn(ALOAD, 2);
            mv2.visitTypeInsn(INSTANCEOF, Type.getInternalName(hierarchy.getTop()));

            Label l5 = new Label();
            mv2.visitJumpInsn(IFEQ, l5);

            Label l6 = new Label();
            mv2.visitLabel(l6);
            mv2.visitVarInsn(ALOAD, 3);
            mv2.visitVarInsn(ALOAD, 2);
            mv2.visitTypeInsn(CHECKCAST, Type.getInternalName(hierarchy.getTop()));
            mv2.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(targetClass), processEventMethodName,
                    "(" + Type.getDescriptor(hierarchy.getTop()) + ")V");
            mv2.visitJumpInsn(Opcodes.GOTO, aroundLabel);
            mv2.visitLabel(l5);

            if (first) {
                mv2.visitFrame(F_APPEND, 1, new Object[] { Type.getInternalName(targetClass) }, 0, null);
                first = false;
            } else {
                mv2.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }
        }
        addErrorLogStatement(mv2);
        if (eventHierarchies.size() > 0) {
            mv2.visitLabel(aroundLabel);
            mv2.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        }
        mv2.visitInsn(RETURN);
        Label l8 = new Label();
        mv2.visitLabel(l8);
        mv2.visitLocalVariable("pe", Type.getDescriptor(ProcessingElement.class), null, l3, l8, 1);
        mv2.visitLocalVariable("event", Type.getDescriptor(Event.class), null, l3, l8, 2);
        mv2.visitLocalVariable("typedPE", Type.getDescriptor(targetClass), null, l3, l8, 3);
        mv2.visitMaxs(4, 4);
        mv2.visitEnd();
    }

    private void addErrorLogStatement(MethodVisitor mv2) {
        mv2.visitVarInsn(ALOAD, 0);
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Object", "getClass", "()Ljava/lang/Class;");
        mv2.visitMethodInsn(INVOKESTATIC, "org/slf4j/LoggerFactory", "getLogger",
                "(Ljava/lang/Class;)Lorg/slf4j/Logger;");
        mv2.visitTypeInsn(NEW, "java/lang/StringBuilder");
        mv2.visitInsn(DUP);
        mv2.visitLdcInsn("Cannot dispatch event of type [");
        mv2.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "(Ljava/lang/String;)V");
        mv2.visitVarInsn(ALOAD, 2);
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Object", "getClass", "()Ljava/lang/Class;");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Class", "getName", "()Ljava/lang/String;");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
                "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
        mv2.visitLdcInsn("] to PE of type [");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
                "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
        mv2.visitVarInsn(ALOAD, 1);
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Object", "getClass", "()Ljava/lang/Class;");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Class", "getName", "()Ljava/lang/String;");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
                "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
        mv2.visitLdcInsn("] : no matching onEvent method found");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
                "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
        mv2.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;");
        mv2.visitMethodInsn(INVOKEINTERFACE, "org/slf4j/Logger", "error", "(Ljava/lang/String;)V");
    }

    // useful for classifying event classes by most specialized
    static class Hierarchy implements Comparable<Hierarchy> {
        private final List<Class<?>> classes = new ArrayList<Class<?>>();

        public Hierarchy(Class<?> clazz) {
            for (Class<?> currentClass = clazz; currentClass != null; currentClass = currentClass.getSuperclass()) {
                classes.add(currentClass);
            }
        }

        public Class<?> getTop() {
            if (classes.size() < 1) {
                return null;
            }
            return classes.get(0);
        }

        public boolean equals(Hierarchy other) {
            if (classes.size() != other.classes.size()) {
                return false;
            }

            for (int i = 0; i < classes.size(); i++) {
                if (!classes.get(i).equals(other.classes.get(i))) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int compareTo(Hierarchy other) {
            if (this.equals(other)) {
                return 0;
            } else if (this.containsClass(other.getTop())) {
                return -1;
            }

            return 1;
        }

        private boolean containsClass(Class<?> other) {
            for (Class<?> clazz : classes) {
                if (clazz.equals(other)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 
     * Delegates to S4Classloader if it was used to load the PE prototype class, by passing to S4Classloader the
     * generated bytecode.
     * 
     * Falls back to parent classloader otherwise.
     * 
     */
    private static class OverloadDispatcherClassLoader extends ClassLoader {

        ClassLoader s4AppLoader;

        public OverloadDispatcherClassLoader(ClassLoader s4AppLoader) {
            this.s4AppLoader = s4AppLoader;
        }

        public Class<?> loadClassFromBytes(String name, byte[] bytes) {

            if (s4AppLoader instanceof S4RLoader) {
                return ((S4RLoader) s4AppLoader).loadGeneratedClass(name, bytes);
            } else {
                try {
                    return this.loadClass(name);
                } catch (ClassNotFoundException cnfe) {
                    // expected
                }
                return this.defineClass(name, bytes, 0, bytes.length);
            }
        }

    }
}
