---
title: Inject events into S4
---

> How do we inject events into S4?

# Problem statement
There is a data stream somewhere. We want to inject that data into our S4 stream processing application.

The data stream can be almost anything: user clicks in a web application, alarms in a monitoring system, stock trade operations, or data from a database or a file system.

# Requirements
* We must be able to connect to that stream of data
* We must be able to chunk the data and isolate events out of it

# Solutions
Currently S4 does not provide a specific abstraction for a source of event or an external data stream. This means that the connection to an external data source can be implemented arbitrarily. Typically, connections to external data sources are initiated in the App object itself, in the `start()` method. 

## In a single application
Let us consider a single S4 application. Remember, the App class is instantiated on each node (for instantiating the local topology). This has some implications. For instance, if we get the information from the twitter sprinker stream, given that the stream is provided through a single socket connection, we should connect only from one node. To ensure that, leader election through ZooKeeper could be an option, but a much simpler one is simply to base this decision on the id of the partition, provided in the `App` class by `getReceiver().getPartitionId()`. In our example, we would initiate the connection only if we are in partition 0.

>The code from the twitter adapter app example could actually be factored into the twitter-counter example App class itself, by listening to the sprinkler stream from partition 0 (for instance)


## Using an adapter application

In some cases the above solution is not practical: it may introduce some load imbalance between the partitions, the extraction of the events out of an external source may be complex, and we might want to enrich the events with other information. In that case, it could be useful to delegate these tasks to a separate application: the adapter application.

Adapter apps can take advantage of the AdapterApp class that automatically creates an output stream. One can define an arbitrary complex graph of PEs and simply use the output stream (`getOutputStream`) to pass events to downstream S4 apps.

S4 applications communicate by matching names of input and output streams. The current mechanism is very simple : when an application creates an output stream, it is exposed in ZooKeeper and other applications that define input streams with the same name are automatically connected, through that matching stream.

By default, events are dispatched in a round-robin fashion between S4 apps.

The [twitter example](../twitter_trending_example) uses an adapter application (though a very basic one).

More info on event dispatch is also available [here](../event_dispatch).




