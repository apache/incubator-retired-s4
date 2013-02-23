---
title: Walkthrough
---


> Improvements from S4 0.5.0 include a more convenient configuration system, illustrated here: all platform and application parameters are specified when configuring/deploying the app.


# Install S4

There are 2 ways:

* [Download](http://incubator.apache.org/s4/download/) the 0.6.0 release 

> We recommend getting the "source" release and building it

* or checkout from the Apache git repository, by following the [instructions](/contrib). The 0.6.0 tag corresponds to the current release.

If you get the binary release, s4 scripts are immediately available. Otherwise you must build the project:

* Compile and install S4 in the local maven repository: (you can also let the tests run without the -DskipTests option)

		S4:incubator-s4$ ./gradlew install -DskipTests
		.... verbose logs ...

* Build the startup scripts:

		S4:incubator-s4$ ./gradlew s4-tools:installApp
		.... verbose logs 
		...:s4-tools:installApp


----


# Start a new application

S4 provides some scripts in order to simplify development and testing of applications. Let's see how to create a new project and start a sample application.

## Create a new project

* Create a new application template (here, we create it in the /tmp directory):

		S4:incubator-s4$ ./s4 newApp myApp -parentDir=/tmp
		... some instructions on how to start ...

* This creates a sample application in the specified directory, with the following structure:

		build.gradle  --> the template build file, that you'll need to customize
		gradlew --> references the gradlew script from the S4 installation
		s4 --> references the s4 script from the S4 installation, and adds an "adapter" task
		src/ --> sources (maven-like structure)


## Have a look at the sample project content

The src/main/java/hello directory contains 3 files:

* HelloPE.java : a very simple PE that simply prints the name contained in incoming events
	// ProcessingElement provides integration with the S4 platform
	public class HelloPE extends ProcessingElement {
	
	    // you should define downstream streams here and inject them in the app definition
	
	    // PEs can maintain some state
	    boolean seen = false;
	
	    // This method is called upon a new Event on an incoming stream.
	    // You may overload it for handling instances of your own specialized subclasses of Event
	    public void onEvent(Event event) {
	        System.out.println("Hello " + (seen ? "again " : "") + event.get("name") + "!");
	        seen = true;
	    }
		// skipped remaining methods

* HelloApp.java: defines a simple application: exposes an input stream ("names"), connected to the HelloPE. See [the event dispatch configuration page](event_dispatch) for more information about how events are dispatched.
	// App parent class provides integration with the S4 platform
	public class HelloApp extends App {
	
	    @Override
	    protected void onStart() {
	    }
	
	    @Override
	    protected void onInit() {
	        // That's where we define PEs and streams
	        // create a prototype
	        HelloPE helloPE = createPE(HelloPE.class);
	        // Create a stream that listens to the "lines" stream and passes events to the helloPE instance.
	        createInputStream("names", new KeyFinder<Event>() {
	                // the KeyFinder is used to identify keys
	            @Override
	            public List<String> get(Event event) {
	                return Arrays.asList(new String[] { event.get("name") });
	            }
	        }, helloPE);
	    }
	// skipped remaining methods

* HelloInputAdapter is a simple adapter that reads character lines from a socket, converts them into events, and sends the events to interested S4 apps, through the "names" stream

## Run the sample app

In order to run an S4 application, you need :

* to set-up a cluster: provision a cluster and start S4 nodes for that cluster
* to package the app
* to publish the app on the cluster

# Set-up the cluster:

* In 2 steps:

	1. Start a Zookeeper server instance (-clean option removes previous ZooKeeper data, if any):
	
			S4:incubator-s4$ ./s4 zkServer - clean
			S4:myApp$ calling referenced s4 script : /Users/S4/tmp/incubator-s4/s4
			[main] INFO  org.apache.s4.tools.ZKServer - Starting zookeeper server on port [2181]
			[main] INFO  org.apache.s4.tools.ZKServer - cleaning existing data in [/var/folders/8V/8VdgKWU3HCiy2yV4dzFpDk+++TI/-Tmp-/tmp/zookeeper/data] and [/var/folders/8V/8VdgKWU3HCiy2yV4dzFpDk+++TI/-Tmp-/tmp/zookeeper/log]

	1. Define a new cluster. Say a cluster named "cluster1" with 2 partitions, nodes listening to ports starting from 12000:

			S4:myApp$ ./s4 newCluster -c=cluster1 -nbTasks=2 -flp=12000
			calling referenced s4 script : /Users/S4/tmp/incubator-s4/s4
			[main] INFO  org.apache.s4.tools.DefineCluster - preparing new cluster [cluster1] with [2] node(s)
			[main] INFO  org.apache.s4.tools.DefineCluster - New cluster configuration uploaded into zookeeper

* Alternatively you may combine these two steps into a single one, by passing the cluster configuration inline with the `zkServer` command:
			
		S4:incubator-s4$ ./s4 zkServer -clusters=c=cluster1:flp=12000:nbTasks=2 -clean

* Start 2 S4 nodes with the default configuration, and attach them to cluster "cluster1" :

		S4:myApp$ ./s4 node -c=cluster1
		calling referenced s4 script : /Users/S4/tmp/incubator-s4/s4
		15:50:18.996 [main] INFO  org.apache.s4.core.Main - Initializing S4 node with :
		- comm module class [org.apache.s4.comm.DefaultCommModule]
		- comm configuration file [default.s4.comm.properties from classpath]
		- core module class [org.apache.s4.core.DefaultCoreModule]
		- core configuration file[default.s4.core.properties from classpath]
		-extra modules: []
		[main] INFO  org.apache.s4.core.Main - Starting S4 node. This node will automatically download applications published for the cluster it belongs to
and again (maybe in another shell):
		
		S4:myApp$ ./s4 node -c=cluster1

* Build, package and publish the app to cluster1:
	* This is done in 2 separate steps:
		1. Create an s4r archive. The following creates an archive named myApp.s4r (here you may specify an arbitrary name) in build/libs.
Again specifying the app class is optional : 

				./s4 s4r -a=hello.HelloApp -b=`pwd`/build.gradle myApp
	
		1. Publish the s4r archive (you may first copy it to a more adequate place). The name of the app is arbitrary: 
		
				./s4 deploy -s4r=`pwd`/build/libs/myApp.s4r -c=cluster1 -appName=myApp

* S4 nodes will detect the new application, download it, load it and start it. You will get something like:

		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s.d.DistributedDeploymentManager - Detected new application(s) to deploy {}[myApp]
		[ZkClient-EventThread-15-localhost:2181] INFO  org.apache.s4.core.Server - Local app deployment: using s4r file name [myApp] as application name
		[ZkClient-EventThread-15-localhost:2181] INFO  org.apache.s4.core.Server - App class name is: hello.HelloApp
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClusterFromZK - Changing cluster topology to { nbNodes=0,name=unknown,mode=unicast,type=,nodes=[]} from null
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClusterFromZK - Adding topology change listener:org.apache.s4.comm.tcp.TCPEmitter@79b2591c
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s.comm.topology.AssignmentFromZK - New session:87684175268872203; state is : SyncConnected
		[ZkClient-EventThread-19-localhost:2181] INFO  o.a.s4.comm.topology.ClusterFromZK - Changing cluster topology to { nbNodes=1,name=cluster1,mode=unicast,type=,nodes=[{partition=0,port=12000,machineName=myMachine.myNetwork,taskId=Task-0}]} from { nbNodes=0,name=unknown,mode=unicast,type=,nodes=[]}
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s.comm.topology.AssignmentFromZK - Successfully acquired task:Task-1 by myMachine.myNetwork
		[ZkClient-EventThread-19-localhost:2181] INFO  o.a.s4.comm.topology.ClusterFromZK - Changing cluster topology to { nbNodes=2,name=cluster1,mode=unicast,type=,nodes=[{partition=0,port=12000,machineName=myMachine.myNetwork,taskId=Task-0}, {partition=1,port=12001,machineName=myMachine.myNetwork,taskId=Task-1}]} from { nbNodes=1,name=cluster1,mode=unicast,type=,nodes=[{partition=0,port=12000,machineName=myMachine.myNetwork,taskId=Task-0}]}
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClustersFromZK - New session:87684175268872205
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClustersFromZK - Detected new stream [names]
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClustersFromZK - New session:87684175268872206
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s4.comm.topology.ClusterFromZK - Changing cluster topology to { nbNodes=2,name=cluster1,mode=unicast,type=,nodes=[{partition=0,port=12000,machineName=myMachine.myNetwork,taskId=Task-0}, {partition=1,port=12001,machineName=myMachine.myNetwork,taskId=Task-1}]} from null
		[ZkClient-EventThread-15-localhost:2181] INFO  org.apache.s4.core.Server - Loaded application from file /tmp/deploy-test/cluster1/myApp.s4r
		[ZkClient-EventThread-15-localhost:2181] INFO  o.a.s.d.DistributedDeploymentManager - Successfully installed application myApp
		[ZkClient-EventThread-15-localhost:2181] DEBUG o.a.s.c.g.OverloadDispatcherGenerator - Dumping generated overload dispatcher class for PE of class [class hello.HelloPE]
		[ZkClient-EventThread-15-localhost:2181] DEBUG o.a.s4.comm.topology.ClustersFromZK - Adding input stream [names] for app [-1] in cluster [cluster1]
		[ZkClient-EventThread-15-localhost:2181] INFO  org.apache.s4.core.App - Init prototype [hello.HelloPE].


Great! The application is now deployed on 2 S4 nodes.

You can check the status of the application, nodes and streams with the "status" command:

	./s4 status

Now what we need is some input!

We can get input through an adapter, i.e. an S4 app that converts an external stream into S4 events, and injects the events into S4 clusters. In the sample application, the adapter is a very basic class, that extends App, listens to an input socket on port 15000, and converts each received line of characters into a generic S4 event, in which the line data is kept in a "name" field. We specify :

* the adapter class
* the name of the output stream
* the cluster where to deploy this app

For easy testing, we provide a facility to start a node with an adapter app without having to package the adapter app.

* First, we need to define a new S4 subcluster for that app:
	
		S4:myApp$ ./s4 newCluster -c=cluster2 -nbTasks=1 -flp=13000

* Then we configure the application:
	* we specify the adapter class (app class)
 	* we use "names" for identifying the output stream (this is the same name used as input by the myApp app)
	* _there is also a -s4r parameter, indicating where to fetch the application package from. We don't need it here, since we skip that step and use a special "adapter" tool_

			./s4 deploy -appClass=hello.HelloInputAdapter -p=s4.adapter.output.stream=names -c=cluster2 -appName=adapter

* Then we simply start the adapter (there is no packaging and copying of the S4R package)
> The adapter command must be run from the root of your S4 project (myApp dir in our case).

		./s4 adapter -c=cluster2

* Now let's just provide some data to the external stream (our adapter is listening to port 15000):
		
		S4:~$ echo "Bob" | nc localhost 15000
		
* One of the nodes should output in its console:
		
		Hello Bob!


> If you keep sending messages, nodes will alternatively display the "hello" messages because the adapter app sends keyless events on the "names" stream in a round-robin fashion by default.

## What happened?

The following figures illustrate the various steps we have taken. The local file system is used as the S4 application repository in our example.

![image](/images/doc/0.6.0/sampleAppDeployment.png)


----


# Run the Twitter trending example

Let's have a look at another application, that computes trendy Twitter topics by listening to the spritzer stream from the Twitter API. This application was adapted from a previous example in S4 0.3.

## Overview

This application is divided into:

* twitter-counter , in test-apps/twitter-counter/ : extracts topics from tweets and maintains a count of the most popular ones, periodically dumped to disk
* twitter-adapter, in test-apps/twitter-adapter/ : listens to the feed from Twitter, converts status text into S4 events, and passes them to the "RawStatus" stream

Have a look at the code in these directories. You'll note that:

* the build.gradle file must be tailored to include new dependencies (twitter4j libs in twitter-adapter)
* events are partitioned through various keys

## Run it!

> Note: You need a twitter4j.properties file in your home directory with the following content (debug is optional):

		debug=true
		user=<a twitter username>
		password=<matching password>

* Start a Zookeeper instance. From the S4 base directory, do:
	
		./s4 zkServer

* Define 2 clusters : 1 for deploying the twitter-counter app, and 1 for the adapter app

		./s4 newCluster -c=cluster1 -nbTasks=2 -flp=12000; ./s4 newCluster -c=cluster2 -nbTasks=1 -flp=13000
		
* Start 2 app nodes (you may want to start each node in a separate console) :

		./s4 node -c=cluster1
		./s4 node -c=cluster1

* Start 1 node for the adapter app:

		./s4 node -c=cluster2 -p=s4.adapter.output.stream=RawStatus
		
* Deploy twitter-counter app (you may also first build the s4r then publish it, as described in the previous section)

		./s4 deploy -appName=twitter-counter -c=cluster1 -b=`pwd`/test-apps/twitter-counter/build.gradle
		
* Deploy twitter-adapter app. In this example, we don't directly specify the app class of the adapter, we use the deployment approach for apps (remember, the adapter is also an app).

		./s4 deploy -appName=twitter-adapter -c=cluster2 -b=`pwd`/test-apps/twitter-adapter/build.gradle
		
* Observe the current 10 most popular topics in file TopNTopics.txt. The file gets updated at regular intervals, and only outputs topics with a minimum of 10 occurrences, so you may have to wait a little before the file is updated :

		tail -f TopNTopics.txt
		
* You may also check the status of the S4 node with:

		./s4 status

----

# What next?

You have now seen some basics applications, and you know how to run them, and how to get events into the system. You may now try to code your own apps with your own data.

[This page](../application_dependencies) will help for specifying your own dependencies.

There are more parameters available for the scripts (typing the name of the task will list the options). In particular, if you want distributed deployments, you'll need to pass the Zookeeper connection strings when you start the nodes.

You may also customize the communication and the core layers of S4 by tweaking configuration files and modules.

Last, the [javadoc](http://people.apache.org/~mmorel/apache-s4-0.6.0-incubating-doc/javadoc/) will help you when writing applications.

We hope this will help you start rapidly, and remember: we're happy to help!