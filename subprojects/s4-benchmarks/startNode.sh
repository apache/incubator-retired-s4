#!/bin/bash -x
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
