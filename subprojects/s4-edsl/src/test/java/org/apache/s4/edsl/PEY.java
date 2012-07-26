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

public class PEY extends ProcessingElement {

    private Stream<EventA>[] stream3;
    @SuppressWarnings("unused")
    private Stream<EventA>[] heightpez;

    private int height;
    private long duration;

    public PEY(App app) {
        super(app);
    }

    @Override
    public void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onRemove() {
        // TODO Auto-generated method stub

    }

    /**
     * @return the stream3
     */
    Stream<EventA>[] getStream3() {
        return stream3;
    }

    /**
     * @param stream3
     *            the stream3 to set
     */
    void setStream3(Stream<EventA>[] stream3) {
        this.stream3 = stream3;
    }

    /**
     * @return the height
     */
    int getHeight() {
        return height;
    }

    /**
     * @param height
     *            the height to set
     */
    void setHeight(int height) {
        this.height = height;
    }

    /**
     * @return the duration
     */
    long getDuration() {
        return duration;
    }

    /**
     * @param duration
     *            the duration to set
     */
    void setDuration(long duration) {
        this.duration = duration;
    }

}
