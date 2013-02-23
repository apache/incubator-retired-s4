---
title: Development tips
---

Here are a few tips to ease the development of S4 applications.


### Import an S4 project into your IDE

You can run `gradlew eclipse` or `gradlew idea` at the root of your S4 application directory. Then simply import the project into eclipse or intellij. You'll have both your application classes _and_ S4 libraries imported to the classpath of the project.

In order to get the transitive dependencies of the platform included as well, you should:

* Download a source distribution
* Install S4 and its dependencies in your local maven repository

		// from s4 source distribution root directory
		./gradlew install -DskipTests
* Then run `gradlew eclipse` or `gradlew idea`




### Start a local Zookeeper instance

* Use the default test configuration (2 clusters with following configs: `c=testCluster1:flp=12000:nbTasks=1` and `c=testCluster2:flp=13000:nbTasks=1`)

		s4 zkServer -t
* Start a Zookeeper instance with your custom configuration, e.g. with 1 partition:
		
		s4 zkServer -clusters=c=testCluster1:flp=12000:nbTasks=1


### Load an application in a new node directly from an IDE

This allows to *skip the packaging phase!*

A requirement is that you have both the application classes and the S4 classes in your classpath. See above.

Then you just need to run the `org.apache.s4.core.Main` class and pass:

* the cluster name: `-c=testCluster1`
* the app class name: `-appClass=myAppClass`

If you use a local Zookeeper instance, there is no need to specify the `-zk` option.