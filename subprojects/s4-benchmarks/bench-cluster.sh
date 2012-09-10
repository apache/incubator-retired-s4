#!/bin/bash -x


HOSTS=$1
INJECTOR_CONFIG=$2
NODE_CONFIG=$3
BENCH_ROOTDIR=`pwd`

echo "hosts = $HOSTS"
echo "injector config file = $INJECTOR_CONFIG"
echo "node config file = $NODE_CONFIG"
echo "bench root dir = $BENCH_ROOTDIR"

killall -9 java

cd $BENCH_ROOTDIR

rm -Rf measurements/*

$BENCH_ROOTDIR/../../gradlew -b=s4-benchmarks.gradle compileJava
$BENCH_ROOTDIR/../../gradlew -b=s4-benchmarks.gradle cp

NB_NODES=0
for host in $HOSTS
do
	((NB_NODES++))
done

(cd $BENCH_ROOTDIR/../../ && ./s4 zkServer -clusters=c=testCluster1:flp=12000:nbTasks=1,c=testCluster2:flp=13000:nbTasks=$NB_NODES &)


sleep 6

BENCH=`date +"%Y-%m-%d--%H-%M-%S"`
BENCH_DIR=$BENCH_ROOTDIR/$BENCH
echo "bench dir is: $BENCH_DIR"
mkdir $BENCH

echo "nb nodes = $NB_NODES\n" > $BENCH/benchConf.txt
echo "hosts = $HOSTS" >> $BENCH/benchConf.txt
echo "injector config ">> $BENCH/benchConf.txt
cat $INJECTOR_CONFIG >> $BENCH/benchConf.txt

for host in $HOSTS
do
  if [ $host == "localhost" ] || [ $host == "127.0.0.1" ] ; then
    $BENCH_ROOTDIR/startNode.sh $BENCH_ROOTDIR $NODE_CONFIG "localhost" > $BENCH_DIR/output_$i.log 2>$BENCH_DIR/s4err_$i.err < /dev/null &
  else
    ssh $host "$BENCH_ROOTDIR/startNode.sh $BENCH_ROOTDIR $NODE_CONFIG $host > $BENCH_DIR/output_$host.log 2>$BENCH_DIR/s4err_$host	.err < /dev/null &"
  fi
done

sleep 15

java -cp `cat classpath.txt` org.apache.s4.core.Main "@$INJECTOR_CONFIG"



