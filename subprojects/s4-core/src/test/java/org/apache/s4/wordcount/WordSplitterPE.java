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

package org.apache.s4.wordcount;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

public class WordSplitterPE extends ProcessingElement {

    private Stream<WordSeenEvent> wordSeenStream;

    public WordSplitterPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {
        String[] split = event.get("sentence").split(" ");
        for (String word : split) {
            wordSeenStream.put(new WordSeenEvent(word));
        }
    }

    public void setWordSeenStream(Stream<WordSeenEvent> stream) {
        this.wordSeenStream = stream;
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
