---
title: Twitter trending example
---
> The [walkthrough](../walkthrough) describes a very basic example; here is a more realistic one

# Twitter trending example

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

* Start a Zookeeper clean instance. From the S4 base directory, do:
	
		./s4 zkServer -clean

* Define 2 clusters : 1 for deploying the twitter-counter app, and 1 for the adapter app

		./s4 newCluster -c=cluster1 -nbTasks=2 -flp=12000; ./s4 newCluster -c=cluster2 -nbTasks=1 -flp=13000
		
* Start 2 app nodes (you may want to start each node in a separate console) :

		./s4 node -c=cluster1
		./s4 node -c=cluster1

* Start 1 node for the adapter app:

		./s4 node -c=cluster2
		
* Build and deploy twitter-counter app

		./s4 s4r -b=`pwd`/test-apps/twitter-counter/build.gradle -appClass=org.apache.s4.example.twitter.TwitterCounterApp twitter-counter
		
		./s4 deploy -appName=twitter-counter -c=cluster1 -s4r=`pwd`/test-apps/twitter-counter/build/libs/twitter-counter.s4r
		
* Build and deploy twitter-adapter app. In this example, we don't directly specify the app class of the adapter, we use the deployment approach for apps (remember, the adapter is also an app). 

		./s4 s4r -b=`pwd`/test-apps/twitter-adapter/build.gradle -appClass=org.apache.s4.example.twitter.TwitterInputAdapter twitter-adapter
		
		./s4 deploy -appName=twitter-adapter -c=cluster2 -s4r=`pwd`/test-apps/twitter-adapter/build/libs/twitter-adapter.s4r -p=s4.adapter.output.stream=RawStatus
		
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

----
