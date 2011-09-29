/*
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.example.counter;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import io.s4.core.App;
import io.s4.core.ProcessingElement;
import io.s4.core.Stream;
import io.s4.core.StreamFactory;

/*
 * This is an sample application to test a new A4 API. 
 * See README file for details.
 * 
 * */

final public class MyApp extends App {

    final private int interval;
    private ProcessingElement generateUserEventPE;
    @Inject private StreamFactory streamFactory;


    /*
     * We use Guice to pass parameters to the application. This is just a
     * trivial example where we get the value for the variable interval from a
     * properties file. (Saved under "src/main/resources".) All configuration
     * details are done in Module.java.
     * 
     * The application graph itself is created in this Class. However,
     * developers may provide tools for creating apps which will generate the
     * objects.
     * 
     * IMPORTANT: we create a graph of PE prototypes. The prototype is a class
     * instance that is used as a prototype from which all PE instance will be
     * created. The prototype itself is not used as an instance. (Except when
     * the PE is of type Singleton PE). To create a data structure for each PE
     * instance you must do in the method ProcessingElement.initPEInstance().
     */
    @Inject
    public MyApp(@Named("pe.counter.interval") int interval) {
        this.interval = interval;
    }

    /*
     * Build the application graph using POJOs. Don't like it? Write a nice
     * tool.
     * 
     * @see io.s4.App#init()
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void init() {

        /* PE that prints counts to console. */
        ProcessingElement printPE = new PrintPE(this);

        Stream<CountEvent> userCountStream = streamFactory.create(this,
                        "User Count Stream", new CountKeyFinder(), printPE);
        Stream<CountEvent> genderCountStream = streamFactory.create(this,
                "Gender Count Stream", new CountKeyFinder(), printPE);
        Stream<CountEvent> ageCountStream = streamFactory.create(this,
                "Age Count Stream", new CountKeyFinder(), printPE);

        /* PEs that count events by user, gender, and age. */
        ProcessingElement userCountPE = new CounterPE(this, interval,
                userCountStream);
        ProcessingElement genderCountPE = new CounterPE(this, interval,
                genderCountStream);
        ProcessingElement ageCountPE = new CounterPE(this, interval,
                ageCountStream);

        /* Streams that output user events keyed on user, gender, and age. */
        Stream<UserEvent> userStream = streamFactory.create(this,
                "User Stream", new UserIDKeyFinder(),userCountPE);
        Stream<UserEvent> genderStream = streamFactory.create(this,
                "Gender Stream", new GenderKeyFinder(), genderCountPE);
        Stream<UserEvent> ageStream = streamFactory.create(this, "Age Stream",
                new AgeKeyFinder(), ageCountPE);

        generateUserEventPE = new GenerateUserEventPE(this, userStream,
                genderStream, ageStream);
    }

    /*
     * Create and send 1000 dummy events of type UserEvent.
     * 
     * @see io.s4.App#start()
     */
    @Override
    protected void start() {

        for (int i = 0; i < 200; i++) {
            generateUserEventPE.processOutputEvent(null);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("Done. Closing...");
        removeAll();

    }

    @Override
    protected void close() {
        System.out.println("Bye.");

    }

    public static void main(String[] args) {

        Injector injector = Guice.createInjector(new Module());
        MyApp myApp = injector.getInstance(MyApp.class);
        myApp.init();
        myApp.start();
    }
}
