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

import java.util.concurrent.TimeUnit;

public class MyApp extends BuilderS4DSL {

    @Override
    public void onInit() {

        pe("PEZ").type(PEZ.class).fireOn(EventA.class).afterInterval(5, TimeUnit.SECONDS).cache().size(1000)
                .expires(3, TimeUnit.HOURS).emit(EventB.class).to("PEX").

                pe("PEY").type(PEY.class).prop("duration", "4").prop("height", "99").timer()
                .withPeriod(2, TimeUnit.MINUTES).emit(EventA.class).onField("stream3")
                .withKeyFinder(DurationKeyFinder.class).to("PEZ").emit(EventA.class).onField("heightpez")
                .withKeyFinder(HeightKeyFinder.class).to("PEZ").

                pe("PEX").type(PEX.class).prop("query", "money").cache().size(100).expires(1, TimeUnit.MINUTES)
                .asSingleton().emit(EventB.class).withKeyFinder(QueryKeyFinder.class).to("PEY", "PEZ").

                build();
    }

}
