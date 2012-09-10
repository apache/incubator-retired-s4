#!/bin/bash -x
BENCH_ROOTDIR=$1
NODE_CONFIG=$2
host=$3

if [ "$host" == "localhost" ] || [ "$host" == "127.0.0.1" ] ; then
  echo "start on localhost"
else
  killall -9 java
fi

cd $BENCH_ROOTDIR


# you may add profiling to the application nodes using the correct options for your system
#PROFILING_OPTS="-agentpath:/Applications/YourKit_Java_Profiler_11.0.8.app/bin/mac/libyjpagent.jnilib=delay=10000,onexit=snapshot,onexit=memory,sampling,monitors"
PROFILING_OPTS=""

java $PROFILING_OPTS -server -cp `cat classpath.txt` org.apache.s4.core.Main "@$NODE_CONFIG" &

# java -cp `cat classpath.txt` org.apache.s4.core.Main "@`pwd`/src/main/resources/injector.config"