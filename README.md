Apache S4
=========

Integration with Helix
----------------------

Goal is to provide better partition management, fault tolerance and automatic rebalancing during cluster expansion.

Currently S4 has the limitation that the number of partitions is always dependent on the number of nodes. In other words 
   * If the stream is already partitioned upstream and if the processing has to be done in S4 but only 5 nodes are sufficient, the stream needs to be re-partitioned which results in additional hop.
   * When the system needs to scale, adding new nodes mean the number of partitions change. This results in lot of data shuffling and possibly losing all the state that is stored.
   * Also the fault tolerance is achieved by having stand alone nodes that remain idle and become active when a node fails. This results in inefficient use of hardware resources.
   
Integrating with Apache Helix, allows one to partition the task processing differently for each stream. Allows adding new nodes without having to change the number of partitions. Allow the fault tolerance can be at a partition level which means all nodes can be active and when a node fails its load can be equally balanced amongst the remaining active nodes.


This is still in prototype mode.

To try it, 

# This will install the helix jars into local repo
   git clone git://github.com/apache/incubator-helix.git
   ./build or mvn clean install -Dmaven.test.exec.skip=true

# Checkout the integration with Helix code
   git clone git://github.com/kishoreg/incubator-s4.git

The following things can be directly run from eclipse.
 
#Create the cluster, -nbTasks is just the number of nodes.
# This will create two nodes localhost_12000 and localhost_12001
   DefineCluster -c=cluster1 -nbTasks=2 -flp=12000

#Create a stream(names) consumer/processor task. Id can be anything but should be unique, for now both needs to be the same,
#p is the number of partitions, so in this case it distributes 4 partitions among two nodes. -r is the number of replica/standby needed for each partition. 

   CreateTask  -zk localhost:2181 -c cluster1 -id names -t consumer -p 4 -r 1 -s names

# Deploy the name by providing the s4r. See the s4 walk through instruction on how to generate this s4r.
   DeployApp -c cluster1 -s4r <incubator-s4>/myApp/build/libs/myApp.s4r -appName HelloApp -zk localhost:2181

# Start the s4 node, note we now need to specify the node id while starting. This gives predictability in which nodes owns the task. After starting this node, you will see that since this is the only node up, it will own all the 4 partitions.
   Main -c=cluster1 -zk=localhost:2181 -id=localhost_12000
   
#Send some events to cluster1. This sends events to the localhost_12000
  GenericEventAdapter 

# Now start another node. Helix automatically detects the new node and assigns 2 out of 4 partitions to the new node. There is a hand off that happens during this part where localhost_12000 can save its state to a common location and localhost_12001 can restore from that state before it accepts new events. This state transfer is not implemented yet.
   Main -c=cluster1 -zk=localhost:2181 -id=localhost_12001

You will see that 50% of the events now go new node localhost_12001


TODO: Add new node.
























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



