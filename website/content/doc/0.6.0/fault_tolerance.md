---
title: Fault tolerance
---
Stream processing applications are typically long running applications, and they may accumulate state over extended periods of time.


Running a distributed system over a long period of time implies there will be:


- failures
- infrastructure updates
- scheduled restarts
- application updates


In each of these situations, some or all of S4 nodes will be shutdown. The system may therefore be partly unavailable, and in-memory state accumulated during the execution may be lost.


In order to deal with this kind of situation, S4 provides:


- high availability
- state recovery (based on checkpointing)
- while preserving low processing latency


In this document, we first describe the high availability mechanism implemented in S4, then we describe the checkpointing and recovery mechanism, and how to customize it, then we describe future improvements.


# Fail-over mechanism

In order to guarantee availability in the presence of sudden node failures, S4 provides a mechanism to automatically detect failed nodes and redirect messages to a standby node.


The following figure illustrates this fail-over mechanism: 

![image](/images/doc/0.6.0/failover.png)


This technique provides high availability but does not prevent state loss.

## Configuration


##### Number of standby nodes

S4 clusters are defined with a fixed number of tasks (\~ partitions). If you have n partitions and start m nodes, with m>n, you get m-n standby nodes.

##### Failure detection timeout

Zookeeper considers a node is dead when it cannot reach it after a the session timeout. The session timeout is specified by the client upon connection, and is at minimum twice the tickTime (heartbeat) specified in the Zookeeper ensemble configuration.


# Checkpointing and recovery

### A closer look at the problem

Upon node crash, the fail-over mechanism brings a new and fresh node to the cluster. When this node is brought into the cluster, it has no state, no instantiated PE. Messages start arriving at this node, and trigger keyed PE instantiations.


If there is no checkpointing and recovery mechanism, those PEs start with an empty state.


### S4 strategy for solving the problem

For PEs to recover a previous state, the technique we use is to:


- *periodically checkpoint* the state of PEs across the S4 cluster
- *lazily recover* (triggered by messages)

This means that if there is a previous state that was checkpointed, and that a new PE is instantiated because a new key is seen, the PE instance will fetch the corresponding checkpoint, recover the corresponding state, and only then start processing events. State loss is minimal!


### Design

##### Checkpointing

In order to minimize the latency, checkpointing is _uncoordinated_ and _asynchronous_.
Uncoordinated checkpointing means that each checkpoint is taken independently, without aiming at global consistency.
Asynchronous checkpointing aims at minimizing the impact on the event processing execution path.


Taking a checkpoint is a 2 steps operations, both handled outside of the event processing path:


- serialize the PE instance
- save the serialized PE instance to remote storage


The following figure shows the various components involved: the checkpointing framework handles the serialization and passes serialized state to a pluggable storage backend:

![image](/images/doc/0.6.0/checkpointing-framework.png)



##### Recovery

In order to optimize the usage of resources, recovery is _lazy_, which means it only happens when necessary.
When a message for a new key arrives in the recovered S4 node, a new PE instance is created, and the system tries to fetch a previous checkpoint from storage. If there is a previous state, it is copied to the newly created PE instance. (This implies deserializing a previous object and copying its fields).


### Configuration and customization

##### Requirements

A PE can be checkpointed if:


- the PE class provides an empty no-arg constructor (that restriction should be lifted in next releases)
- it has non transient serializable fields (and by opposition, transient fields will never be checkpointed)


##### Checkpointing application configuration

Checkpointing intervals are defined per prototype, in time intervals or event counts (for now). This is specified in the application module, using API methods from the ProcessingElement class, and passing a CheckpointingConfiguration object. Please refer to the API documentation.


The twitter example application shipped in the distribution is already configured for enabling checkpointing. See the [TwitterCounterApp](https://git-wip-us.apache.org/repos/asf?p=incubator-s4.git;a=blob;f=test-apps/twitter-counter/src/main/java/org/apache/s4/example/twitter/TwitterCounterApp.java;h=5d7855fa5aee6cbe693fa47c1ebad03da316f42b) class.


For instance, here is how to specify a checkpointing frequency of 20s on the TopNTopicPE prototype:


	topNTopicPE.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.TIME).frequency(20).timeUnit(TimeUnit.SECONDS).build());


##### Enabling checkpointing

This is a node configuration. You need to inject a checkpointing module that speficies a CheckpointingFramework implementation (please use org.apache.s4.core.ft.SafeKeeper) and a backend storage implementation. The backend storage implements the StateStorage interface.


We provide a default module (FileSystemBackendCheckpointingModule) that uses a file system backend (DefaultFileSystemStateStorage). It can be used with an NFS setup and introduces no dependency. You may use it by starting an S4 node in the following manner:


	./s4 node -c=cluster1 -emc=org.apache.s4.core.ft.FileSystemBackendCheckpointingModule


##### Customizing the checkpointing backend

It is quite straightforward to implement backends for other kinds of storage (key value stores, datagrid, cache, RDBMS). Using an alternative backend is as simple as providing a new module to the S4 node. Here is an example of a module using a 'Cool' backend implementation:


	public class CoolBackendCheckpointingModule extends AbstractModule {
		@Override
		protected void configure() {
	    	bind(StateStorage.class).to(CoolStateStorage.class);
	    	bind(CheckpointingFramework.class).to(SafeKeeper.class);
		}
	}


##### Overriding checkpointing and recovery operations

By default, S4 uses [kryo](http://code.google.com/p/kryo) to serialize and deserialize checkpoints, but it is possible to use a different mechanism, by overriding the `checkpoint()`, `serializeState()` and `restoreState()` methods of the `ProcessingElement` class.


PEs are eligible for checkpointing when their state is 'dirty'. The dirty flag is checked through the `isDirty()` method, and cleared by calling the `clearDirty()` method. In some cases, dependent on the application code, only some of the events may actually change the state of the PE. You should override these methods in order to avoid unjustified checkpointing operations.


##### Tuning

The checkpointing framework has a number of overridable parameters, mostly for sizing thread pools:


* Serialization thread pool
	* s4.checkpointing.serializationMaxThreads (default = 1)
	* s4.checkpointing.serializationThreadKeepAliveSeconds (default = 120)
	* s4.checkpointing.serializationMaxOutstandingRequests (default = 1000)

* Storage backend thread pool
	* s4.checkpointing.storageMaxThreads (default = 1)
	* s4.checkpointing.storageThreadKeepAliveSeconds (default = 120)
	* s4.checkpointing.storageMaxOutstandingRequests (default = 1000)

* Fetching thread pool: fetching is a blocking operation, which can timeout:
	* s4.checkpointing.fetchingMaxThreads (default = 1)
	* s4.checkpointing.fetchingThreadKeepAliveSeconds (default = 120)
	* s4.checkpointing.fetchingMaxWaitMs (default = 1000) (corresponds to the timeout)

* In the case the backend is unresponsive, it can be bypassed:
	* s4.checkpointing.fetchingMaxConsecutiveFailuresBeforeDisabling (default = 10)
	* s4.checkpointing.fetchingDisabledDurationMs (default = 600000)