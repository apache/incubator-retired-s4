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

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class PEX extends ProcessingElement {

    private String query;
    private Stream<EventB>[] someStream;
    @SuppressWarnings("unused")
    private Stream<EventA>[] streams;

    public PEX(App app) {
        super(app);
    }

    @Override
    public void onCreate() {

    }

    @Override
    public void onRemove() {

    }

    /**
     * @return the keyword
     */
    String getKeyword() {
        return query;
    }

    /**
     * @param query
     *            the keyword to set
     */
    void setKeyword(String query) {
        this.query = query;
    }

    /**
     * @return the someStream
     */
    public Stream<EventB>[] getSomeStream() {
        return someStream;
    }

    /**
     * @param someStream
     *            the someStream to set
     */
    public void setSomeStream(Stream<EventB>[] someStream) {
        this.someStream = someStream;
    }

}
