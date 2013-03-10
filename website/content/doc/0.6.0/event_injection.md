---
title: Inject events into S4
---

> How do we inject events into S4?

# Problem statement
There is a data stream somewhere. We want to inject that data into our S4 stream processing application.

The data stream can be almost anything: user clicks in a web application, alarms in a monitoring system, stock trade operations, even static data from a database or a file server.

# Requirements
* We must be able to connect to that stream of data
* We must be able to chunk the data and isolate events out of it

# Solutions
Currently S4 does not provide a specific abstraction for a source of event or an external data stream. This means that the connection to an external data source can be implemented arbitrarily. Typically, connections to external data sources are initiated in the App object itself, in the `start()` method. 

## In a single application
Let us consider a single S4 application. The App class is instantiated on each node (for instantiating the local topology), and this has to be taken in consideration.
For instance, if we get the information from the twitter sprinker stream, given that the stream is provided through a single socket connection, we should connect only from one node. To ensure that, leader election through ZooKeeper is an option, but a much simpler one is simply to base this decision on the id of the partition, provided in the `App` class by `getReceiver().getPartitionId()`. In our example, we would initiate the connection only if we are in partition 0.

~~~
#!java



~~~


## Using an adapter application

In some cases the above solution is not practical: it introduces some load imbalance between the partitions, and the extraction of the 




