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

package org.apache.s4.example.model;

import java.util.*;
import java.util.zip.GZIPInputStream;
import java.io.*;

public class DataFile implements Iterable<String> {
    private BufferedReader reader;

    public DataFile(String filePath) throws FileNotFoundException, IOException {

        InputStream is = this.getClass().getResourceAsStream(filePath);

        GZIPInputStream gzip = new GZIPInputStream(is);

        this.reader = new BufferedReader(new InputStreamReader(gzip));
    }

    public void close() throws IOException {
        reader.close();
    }

    public Iterator<String> iterator() {
        return new FileIterator();
    }

    private class FileIterator implements Iterator<String> {
        private String currentLine;

        public boolean hasNext() {
            try {
                currentLine = reader.readLine();
            } catch (Exception ex) {
                currentLine = null;
            }

            return currentLine != null;
        }

        public String next() {
            return currentLine;
        }

        public void remove() {
        }
    }
}
