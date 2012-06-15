An application that displays the current top 10 topics, as gathered from the twitter sample stream.
It was ported and adapted from S4 0.3

Architecture:
- twitter-adapter app in adapter node connects to the twitter stream, extracts the twitted text and passes that to the application cluster
- twitter-counter app in the application cluster receives the text of the tweets, extracts the topics, counts topic occurences and periodically displays the top 10 topics on the console

How to configure:
- you need a twitter4j.properties file in your home dir, with the following properties filled:
debug=true|false
user=<a twitter user name>
password=<the matching password>

How to run:
0/ make sure tools are compiled by running ./gradlew s4-tools:installApp

1/ start zookeeper server
./s4 zkServer

2/ create adapter cluster configuration
./s4 newCluster -name=s4-test-cluster -firstListeningPort=10000 -nbTasks=1

3/ create application cluster configuration
./s4 newCluster -name=s4-adapter-cluster -firstListeningPort=11000 -nbTasks=<number of nodes>
NOTE: - the name of the downstream cluster is currently hardcoded in <s4-dir>/test-apps/twitter-adapter/src/main/resources/s4.properties Make sure you use the same name

4/ start application nodes (as many as defined in the cluster configuration, more for failover capabilities)
./s4 appNode <s4-dir>/subprojects/s4-core/src/test/resources/default.s4.properties

5/ start adapter node
./s4 adapterNode -s4Properties=<s4-dir>/test-apps/twitter-adapter/src/main/resources/s4.properties

6/ observe the topic count output (on 1 of the application nodes)