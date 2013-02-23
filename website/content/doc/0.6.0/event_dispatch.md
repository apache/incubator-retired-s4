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

Stream<TopicEvent> topicSeenStream = createStream("TopicSeen", new KeyFinder<TopicEvent>() {

        @Override
        public List<String> get(final TopicEvent arg0) {
            return ImmutableList.of(arg0.getTopic());
        }
    }, topicCountAndReportPE);


When an event is sent to the "TopicSeen" stream, its key will be identified through the KeyFinder implementation, hashed and dispatched to the matching partition.


The same logic applies when defining _output streams_.

If we use an AdapterApp subclass, the `remoteStreamKeyFinder` should be defined in the `onInit()` method, _before_ calling `super.onInit()`:

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


If we use a standard App, we use the `createOutputStream(String name, KeyFinder<Event> keyFinder)` method.


bq. If the KeyFinder is not defined for the output streams, events are sent to partitions of the connected cluster in a round robin fashion.


# Incoming dispatch from external events

When receiving events from a remote application, we _must_ define how external events are dispatched internally, to which PEs and based on which keys. For that purpose, we simply define and _input stream_ with the corresponding KeyFinder:

createInputStream("names", new KeyFinder<Event>() {

	@Override
	public List<String> get(Event event) {
	    return Arrays.asList(new String[] { event.get("name") });
	   }
	}, helloPE);


In this case, a name is extracted from each event, the PE instance with this key is retrieved or created, and the event sent to that instance.


Alternatively, we can use a unique PE instance for processing events in a given node. For that we simply define the input stream without a KeyFinder, _and_ use a singleton PE:

	HelloPE helloPE = createPE(HelloPE.class);
	helloPE.setSingleton(true);
	createInputStream("names", helloPE);


In this case, all events will be dispatched to the only HelloPE instance in this partition, regardless of the content of the event.

