#/bin/bash -x

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
