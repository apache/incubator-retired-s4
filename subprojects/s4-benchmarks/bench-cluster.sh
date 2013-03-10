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


HOSTS=$1
INJECTOR_CONFIGS=$2 # 1 injector injects to 1 stream. Use more injector configs for injecting to more streams
NODE_CONFIG=$3
NB_INJECTORS_PER_NODE=$4
INJECTOR_NODES=$5
BENCH_ROOTDIR=`pwd`

echo "hosts = $HOSTS"
echo "injector config file = $INJECTOR_CONFIG"
echo "node config file = $NODE_CONFIG"
echo "bench root dir = $BENCH_ROOTDIR"

#########################################################
#### cleanup files and processes, and build platform
#########################################################

killall -9 java

cd $BENCH_ROOTDIR

rm -Rf measurements/*

$BENCH_ROOTDIR/../../gradlew -b=s4-benchmarks.gradle compileJava
$BENCH_ROOTDIR/../../gradlew -b=s4-benchmarks.gradle cp

NB_NODES=0
for host in $HOSTS
do
	((NB_NODES++))
	ssh $host "killall -9 java"
done

NB_INJECTORS=0
for injectorNode in $INJECTOR_NODES ; do
	for INJECTOR_CONFIG in $INJECTOR_CONFIGS ; do
		NB_INJECTORS=$(($NB_INJECTORS + $NB_INJECTORS_PER_NODE))
	done
	ssh $injectorNode "killall -9 java"
done

# must run from where ZooKeeper server is running (as specified in injector config file)
(cd $BENCH_ROOTDIR/../../ && ./s4 zkServer -clusters=c=testCluster1:flp=12000:nbTasks=$NB_INJECTORS,c=testCluster2:flp=13000:nbTasks=$NB_NODES &)


sleep 6

BENCH=`date +"%Y-%m-%d--%H-%M-%S"`
BENCH_DIR=$BENCH_ROOTDIR/$BENCH
echo "bench dir is: $BENCH_DIR"
mkdir $BENCH

echo "nb nodes = $NB_NODES\n" > $BENCH/benchConf.txt
echo "hosts = $HOSTS" >> $BENCH/benchConf.txt
echo "injector config ">> $BENCH/benchConf.txt
for INJECTOR_CONFIG in $INJECTOR_CONFIGS ; do
	cat $INJECTOR_CONFIG >> $BENCH/benchConf.txt
done


#########################################################
#### start S4 nodes
#########################################################

i=0
for host in $HOSTS
do
  ((i++))
  if [ $host == "localhost" ] || [ $host == "127.0.0.1" ] ; then
    $BENCH_ROOTDIR/startNode.sh $BENCH_ROOTDIR $NODE_CONFIG "localhost" > $BENCH_DIR/output_$i.log 2>$BENCH_DIR/s4err_$i.err < /dev/null &
  else
    ssh $host "$BENCH_ROOTDIR/startNode.sh $BENCH_ROOTDIR $NODE_CONFIG $host > $BENCH_DIR/output_$host-$i.log 2>$BENCH_DIR/s4err_$host-$i.err < /dev/null &"
  fi
done

sleep 15

PROFILING_OPTS=""

#########################################################
#### start injectors
#########################################################

for INJECTOR_NODE in $INJECTOR_NODES ; do
	for INJECTOR_CONFIG in $INJECTOR_CONFIGS ; do
		ssh $INJECTOR_NODE "cd $BENCH_ROOTDIR ; $BENCH_ROOTDIR/startInjector.sh $NB_INJECTORS_PER_NODE $INJECTOR_CONFIG $ZK_SERVER > $BENCH_DIR/out.injector_$INJECTOR_NODE.log 2>$BENCH_DIR/err.injector_$INJECTOR_NODE.log < /dev/null &"
	done
done


