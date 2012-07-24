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

public class PEZ extends ProcessingElement {

    private Stream<EventA>[] stream1;
    private Stream<EventB>[] stream2;

    public PEZ(App app) {
        super(app);
    }

    /**
     * @return the stream1
     */
    Stream<EventA>[] getStream1() {
        return stream1;
    }

    /**
     * @param stream1
     *            the stream1 to set
     */
    void setStream1(Stream<EventA>[] stream1) {
        this.stream1 = stream1;
    }

    /**
     * @return the stream2
     */
    Stream<EventB>[] getStream2() {
        return stream2;
    }

    /**
     * @param stream2
     *            the stream2 to set
     */
    void setStream2(Stream<EventB>[] stream2) {
        this.stream2 = stream2;
    }

    @Override
    public void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onRemove() {
        // TODO Auto-generated method stub

    }

}
