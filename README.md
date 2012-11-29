Apache S4
=========

Integration with Helix
----------------------

Goal is to provide better partition management, fault tolerance and automatic rebalancing during cluster expansion.

Limitation in S4
   * Tasks are always partitioned based on the number of nodes
   * If the stream is already partitioned outside of s4 or in a pub-sub system but number of nodes in s4 cluster is different, then the stream needs to be re-partitioned which results in additional hop. In cases where multiple streams that are already partitioned outside of S4 but needs to be joined in S4, it requires re-hashing both the streams.
   * When the system needs to scale, adding new nodes mean the number of partitions change. This results in lot of data shuffling and possibly losing all the state that is stored.
   * Also the fault tolerance is achieved by having stand alone nodes that remain idle and becomes active when a node fails. This results in inefficient use of hardware resources.
   
Integrating with Apache Helix, 
   * Allows one to partition the task processing differently for each stream. 
   * Allows adding new nodes without having to change the number of partitions. 
   * Fault tolerance can be achieved at a partition level which means all nodes can be active and when a node fails its load can be equally balanced among the remaining active nodes.


This is still in prototype mode.

Instruction
-----------

This will install the helix jars into local repo
   
    git clone git://github.com/apache/incubator-helix.git
    ./build or mvn clean install -Dmaven.test.exec.skip=true

Checkout the S4 integration with Helix code    
    
    git clone git://github.com/kishoreg/incubator-s4.git
    
Build S4.

    ./gradlew install
    ./gradlew s4-tools:installApp
    
Start zookeeper

    ./s4 zkServer
 
Create the cluster, -nbNodes is just the number of s4 nodes that will be run. This will create two nodes localhost_12000 and localhost_12001

    ./s4 newCluster -c=cluster1 -nbNodes=2 -flp=12000

Create a task that processes events from stream(names). -id can be anything but should be unique within a cluster, for now id and stream name needs to be the same. p is the number of partitions, so in this case it distributes 4 partitions among two nodes. -r is the number of replica/standby needed for each partition. Note that, when a node fails its load would be distributed among remaining nodes. So even though theoretically its possible to have number of standby's as the number of nodes, the performance would be horrible. In general this can be decided based on the head room available in the cluster.

    ./s4 createTask  -zk localhost:2181 -c cluster1 -id names -t consumer -p 4 -r 1 -s names

Generate a HelloWorld App

     ./s4 newApp myApp -parentDir=/tmp
     cd /tmp/myApp
     ./s4 s4r -a=hello.HelloApp -b=/tmp/myApp/build.gradle myApp
     
Deploy the App by providing the s4r. One can optionally provide the list of nodes where this App has to be deployed.

    ./s4 deployApp -c=cluster1 -s4r=/tmp/myApp/build/libs/myApp.s4r -appName=myApp -zk=localhost:2181

Start the two s4 nodes in two separate windows. Note we now need to specify the node id while starting. This allows nodes to associated with same partitions when they are started. 

    ./s4 node -c=cluster1 -zk=localhost:2181 -id=localhost_12000
    ./s4 node -c=cluster1 -zk=localhost:2181 -id=localhost_12001
    
   
Send some events to names stream. Notice that the partitions are divided among two nodes and each event is routed to appropriate node.

    ./s4 adapter -c=cluster1 -zk=localhost:2181 -s=names


Overview
--------
S4 is a general-purpose, distributed, scalable, partially fault-tolerant, pluggable 
platform that allows programmers to easily develop applications for processing continuous 
unbounded streams of data.

S4 0.5.0 is a complete refactoring of the previous version of S4. It grounds on the same 
concepts (partitioning inspired by map-reduce, actors-like distribution model), 
but with the following objectives:

- cleaner and simpler API
- robust configuration through statically defined modules
- cleaner architecture
- robust codebase
- easier to develop S4 apps, to test, and to use the platform

We added the following core features:

- TCP-based communications
- state recovery through a flexible checkpointing mechanism
- inter-cluster/app communications through a pub-sub model
- dynamic application deployment
- toolset for easily starting S4 nodes, testing, packaging, deploying and monitoring S4 apps 


Documentation
-------------

For the latest information about S4, please visit our website at:

   http://inbubator.apache.org/s4

and our wiki, at:

   https://cwiki.apache.org/confluence/display/S4/S4+Wiki

Currently the wiki contains the most relevant and up-to-date documentation.

Source code is available here: https://git-wip-us.apache.org/repos/asf?p=incubator-s4.git


Requirements
------------
* JDK 6 or higher
* *nix or macosx (you may build the project and develop S4 applications with 
microsoft windows though, the only limitation is that the "s4" script has not 
been ported to windows yet)


How to build
------------
This only applies if you checkout from the source repository or if you download a 
released source package.


We use gradle http://gradle.org as the build system.

* From the root directory of the S4 project:

./gradlew install

This will build the packages and install the artifacts in the local maven repository.

* Then, build the tools:

./gradlew s4-tools:installApp

This will build the tools so that you can drive the platform through the "s4" command.


Directory structure
-------------------
* If you have a  source package:
	- root directory contains build and utility scripts, as well as informative files
	- config/ directory contains configuration files for source code formatting
	- doc/ directory contains the javadoc
	- gradle/ directory contains libraries used by the gradle build tool
	- lib/ directory contains some other gradle libraries 
	- subproject/ directory contains the plaftorm subprojects: base, comm, core, tools, 
	as well as example (example is not fully ported to 0.5.0)
	- test-apps/ directory contains some examples (some of them very trivial are used 
	in regression tests)



* If you have a binary package:
	- root directory contains the s4 utility script as well as informative files
	- doc/ directory contains the javadoc
	- lib/ directory contains :
		* the platform libraries (prefixed with "s4")
		* the dependencies
	- bin/ directory contains some scripts that are used by the s4 script
	- gradle/ directory contains libraries used for building S4 projects



