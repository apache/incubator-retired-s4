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

package org.apache.s4.core.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.internal.Maps;

/**
 * Utilities for parsing command line parameters
 */
public class ParsingUtils {

    public static Map<String, String> convertListArgsToMap(List<String> args) {
        Map<String, String> result = Maps.newHashMap();
        for (String arg : args) {
            String[] split = arg.split("[=]");
            if (!(split.length == 2)) {
                throw new RuntimeException("Invalid args: " + Arrays.toString(args.toArray(new String[] {})));
            }
            result.put(split[0], split[1]);
        }
        return result;
    }

    public static class InlineConfigParameterConverter implements IStringConverter<String> {

        private static Logger logger = LoggerFactory.getLogger(InlineConfigParameterConverter.class);

        @Override
        public String convert(String arg) {
            Pattern parameterPattern = Pattern.compile("(\\S+=\\S+)");
            logger.info("processing inline configuration parameter {}", arg);
            Matcher parameterMatcher = parameterPattern.matcher(arg);
            if (!parameterMatcher.find()) {
                throw new IllegalArgumentException("Cannot understand parameter " + arg);
            }
            return parameterMatcher.group(1);
        }
    }

}
