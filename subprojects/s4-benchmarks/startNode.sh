#!/bin/bash -x
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
BENCH_ROOTDIR=$1
NODE_CONFIG=$2

cd $BENCH_ROOTDIR

############################################################################################
#### start S4 node, using the dumped classpath in order to avoid downloading the application
############################################################################################

# you may add profiling to the application nodes using the correct options for your system
PROFILING_OPTS=""

host=`hostname`

java -Xmx4G -Xms4G -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc-$host.log $PROFILING_OPTS -server -cp `cat classpath.txt` org.apache.s4.core.Main "@$NODE_CONFIG" &
