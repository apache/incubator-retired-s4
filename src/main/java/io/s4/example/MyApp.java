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
package io.s4.example;

import io.s4.App;
import io.s4.ProcessingElement;
import io.s4.Stream;

// each stream can have its own thread for testing
// use blocking queue

final public class MyApp extends App {

    /* Build the application graph. */

    /* PE that prints counts to console. */
    final private ProcessingElement printPE = new PrintPE(this);

    /* Streams that output count events by user, gender, and age. */
    final private Stream<CountEvent> userCountStream = new Stream<CountEvent>(
            "User Count Stream", new CountKeyFinder(), printPE);
    final private Stream<CountEvent> genderCountStream = new Stream<CountEvent>(
            "Gender Count Stream", new CountKeyFinder(), printPE);
    final private Stream<CountEvent> ageCountStream = new Stream<CountEvent>(
            "Age Count Stream", new CountKeyFinder(), printPE);

    /* PEs that count events by user, gender, and age. */
    final private ProcessingElement userCountPE = new CounterPE(this,
            userCountStream);
    final private ProcessingElement genderCountPE = new CounterPE(this,
            genderCountStream);
    final private ProcessingElement ageCountPE = new CounterPE(this,
            ageCountStream);

    /* Streams that output user events keyed on user, gender, and age. */
    final private Stream<UserEvent> userStream = new Stream<UserEvent>(
            "User Stream", new UserIDKeyFinder(), userCountPE);
    final private Stream<UserEvent> genderStream = new Stream<UserEvent>(
            "Gender Stream", new GenderKeyFinder(), genderCountPE);
    final private Stream<UserEvent> ageStream = new Stream<UserEvent>(
            "Age Stream", new AgeKeyFinder(), ageCountPE);

    final private ProcessingElement generateUserEventPE = new
    GenerateUserEventPE(this, userStream, genderStream, ageStream);

    @Override
    protected void create() {

    }

    @Override
    protected void init() {

    }
    
    public static void main(String[] args){
       
        MyApp myApp = new MyApp();
        
        for(int i=0; i<1000; i++) {
            myApp.generateUserEventPE.sendEvent();
        }
    }
}
