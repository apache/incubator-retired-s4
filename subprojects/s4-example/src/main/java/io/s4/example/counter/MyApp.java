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

import java.util.concurrent.TimeUnit;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import io.s4.base.Event;
import io.s4.core.App;
import io.s4.core.Receiver;
import io.s4.core.Sender;
import io.s4.core.Stream;

/*
 * This is a sample application to test a new S4 API. 
 * See README file for details.
 * 
 * */

final public class MyApp extends App {

    final private int interval;
    private GenerateUserEventPE generateUserEventPE;

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
        PrintPE printPE = createPE(PrintPE.class);

        Stream<CountEvent> userCountStream = createStream("User Count Stream",
                new CountKeyFinder(), printPE);
        Stream<CountEvent> genderCountStream = createStream(
                "Gender Count Stream", new CountKeyFinder(), printPE);
        Stream<CountEvent> ageCountStream = createStream("Age Count Stream",
                new CountKeyFinder(), printPE);

        /* PEs that count events by user, gender, and age. */
        CounterPE userCountPE = createPE(CounterPE.class);
        userCountPE.setTrigger(Event.class, interval, 10l, TimeUnit.SECONDS);
        userCountPE.setCountStream(userCountStream);

        CounterPE genderCountPE = createPE(CounterPE.class);
        genderCountPE.setTrigger(Event.class, interval, 10l, TimeUnit.SECONDS);
        genderCountPE.setCountStream(genderCountStream);

        CounterPE ageCountPE = createPE(CounterPE.class);
        ageCountPE.setTrigger(Event.class, interval, 10l, TimeUnit.SECONDS);
        ageCountPE.setCountStream(ageCountStream);

        /* Streams that output user events keyed on user, gender, and age. */
        Stream<UserEvent> userStream = createStream("User Stream",
                new UserIDKeyFinder(), userCountPE);
        Stream<UserEvent> genderStream = createStream("Gender Stream",
                new GenderKeyFinder(), genderCountPE);
        Stream<UserEvent> ageStream = createStream("Age Stream",
                new AgeKeyFinder(), ageCountPE);

        generateUserEventPE = createPE(GenerateUserEventPE.class);
        generateUserEventPE.setStreams(userStream, genderStream, ageStream);
    }

    /*
     * Create and send 200 dummy events of type UserEvent.
     * 
     * @see io.s4.App#start()
     */
    @Override
    protected void start() {

        for (int i = 0; i < 200; i++) {
            generateUserEventPE.onTrigger(null);
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
        Sender sender = injector.getInstance(Sender.class);
        Receiver receiver = injector.getInstance(Receiver.class);
        myApp.setCommLayer(sender, receiver);
        myApp.init();
        myApp.start();
    }
}
