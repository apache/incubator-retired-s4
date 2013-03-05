---
title: Event dispatch
---

Events are dispatched according to their key.

The key is identified in an `Event` through a `KeyFinder`.

Dispatch can be configured for:

* dispatching events to partitions (_outgoing dispatch_)
* dispatching external events within a partition  (_incoming dispatch_)

# Outgoing dispatch

A stream can be defined with a KeyFinder, as :

~~~

#!java

Stream<TopicEvent> topicSeenStream = createStream("TopicSeen", new KeyFinder<TopicEvent>() {
	    @Override
	    public List<String> get(final TopicEvent arg0) {
	        return ImmutableList.of(arg0.getTopic());
	    }
	}, topicCountAndReportPE);

~~~

When an event is sent to the "TopicSeen" stream, its key will be identified through the KeyFinder implementation, hashed and dispatched to the matching partition.


The same logic applies when defining _output streams_.

If we use an AdapterApp subclass, the `remoteStreamKeyFinder` should be defined in the `onInit()` method, _before_ calling `super.onInit()`:

~~~
#!java

    @Override
    protected void onInit() {
    ... 
    remoteStreamKeyFinder = new KeyFinder<Event>() {
        @Override
        public List<String> get(Event event) {
            return ImmutableList.of(event.get("theKeyField"));
        }
    };
    super.onInit()
    ...
~~~

If we use a standard App, we use the `createOutputStream(String name, KeyFinder<Event> keyFinder)` method.


> If the KeyFinder is not defined for the output streams, events are sent to partitions of the connected cluster in a round robin fashion.


# Incoming dispatch from external events

When receiving events from a remote application, we _must_ define how external events are dispatched internally, to which PEs and based on which keys. For that purpose, we simply define and _input stream_ with the corresponding KeyFinder:

~~~
#!java

createInputStream("names", new KeyFinder<Event>() {

    @Override
    public List<String> get(Event event) {
        return Arrays.asList(new String[] { event.get("name") });
       }
    }, helloPE);
~~~

In this case, a name is extracted from each event, the PE instance with this key is retrieved or created, and the event sent to that instance.


Alternatively, we can use a unique PE instance for processing events in a given node. For that we simply define the input stream without a KeyFinder, _and_ use a singleton PE:

~~~
#!java

HelloPE helloPE = createPE(HelloPE.class);
helloPE.setSingleton(true);
createInputStream("names", helloPE);
~~~

In this case, all events will be dispatched to the only HelloPE instance in this partition, regardless of the content of the event.


# Internals and tuning

S4 follows a staged event driven architecture and uses a pipeline of executors to process messages. 

## executors
An executor is an object that executes tasks. It usually keeps a bounded queue of task items and schedules their execution through a pool of threads.

When processing queues are full, executors may adopt various possible behaviours, in particular, in S4:
	* **blocking**: the current thread simply waits until the queue is not full
	* **shedding**: the current event is dropped

**Throttling**, i.e. placing an upper bound on the maximum processing rate, is a convenient way to avoid sending too many messages too fast.

S4 provides various default implementations of these behaviours and you can also define your own custom executors as appropriate.

## workflow

The following picture illustrates the pipeline of executors.

![image](/images/doc/0.6.0/executors.png)

### When a node receives a message:

1. data is received on a socket and chunked into a message, in the form of an array of bytes
1. the message is passed to a deserializer executor
	* this executor is loaded with the application, and therefore has access to application classes, so that application specific messages can be deserialized
	* by default it uses 1 thread and **blocks** if the processing queue is full
1. the event (deserialized message) is dispatched to a stream executor 
	* the stream executor is selected according to the stream information contained in the event
	* by default it **blocks** if the processing queue is full
1. the event is processed in the PE instance that matches the key of the event

### When a PE emits a message:

1. an event is passed to a referenced stream
1. if the target cluster is remote, the event is passed to a remote sender executor
1. otherwise, 
	* if the target partition is the current one, the event is directly passed to the corresponding stream executor (see step 3 above)
	* otherwise, the event is passed to a local sender executor
1. remote or local sender executors serialize the event into an array of bytes
	* remote sender executors are **blocking** by default, if their processing queue is full
	* local sender executors are **throttling** by default, with a configurable maximum rate. If events arrive at a higher rate, they are **dropped**.
	
	
## configuration parameters

* blocking executors can lead to deadlocks, depending on the application graph
* back pressure can be taken into account when using TCP: if downstream systems saturate, messages cannot be sent downstream, and sending queues fill up. With knowledge of the application, is is possible to add some mechanisms to react appropriately
* executors can be replaced by other implementations in custom modules, by overriding the appropriate bindings (see `DefaultCommModule` and `DefaultCodeModule`)
* the maximum number of threads to use to process a given stream can be specified **in the application**, using the `setParallelism()` method of `Stream`
* default parameters are specified in `default.s4.comm.properties`


