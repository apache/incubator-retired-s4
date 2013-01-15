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

package org.apache.s4.edsl;

import java.lang.reflect.Field;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.core.BaseModule;
import org.apache.s4.core.DefaultCoreModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestEDSL extends ZkBasedTest {

    public final static String CLUSTER_NAME = "cluster1";

    @Test
    public void test() throws Exception {
        Injector injector = Guice
                .createInjector(new BaseModule(Resources.getResource("default.s4.base.properties").openStream(),
                        "cluster1"), new DefaultCommModule(Resources.getResource("default.s4.comm.properties")
                        .openStream()), new DefaultCoreModule(Resources.getResource("default.s4.core.properties")
                        .openStream()));
        MyApp myApp = injector.getInstance(MyApp.class);

        /* Normally. the container will handle this but this is just a test. */
        myApp.init();
        myApp.start();
        myApp.close();
    }

    @Test
    public void testReflection() {

        try {
            Class<?> c = PEY.class;
            Field f = c.getDeclaredField("duration");
            System.out.format("Type: %s%n", f.getType());
            System.out.format("GenericType: %s%n", f.getGenericType());

            // production code should handle these exceptions more gracefully
        } catch (NoSuchFieldException x) {
            x.printStackTrace();
        }
    }

}
