#/bin/bash -x
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

NB_INJECTORS_PER_NODE=$1
INJECTOR_CONFIG=$2
ZK_STRING=$3

##############################################################################################
#### start injectors, using the dumped classpath in order to avoid downloading the application
##############################################################################################


#killall -9 java
PROFILING_OPTS=""

host=`hostname`

for ((i = 1; i <= $NB_INJECTORS_PER_NODE; i++)); do
	java $PROFILING_OPTS -Xmx1G -Xms1G -verbose:gc -Xloggc:gc-injector-$host.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -cp `cat classpath.txt` org.apache.s4.core.Main "@$INJECTOR_CONFIG" &
done
