S4-Piper Design
===============

The goal of this project is to incorporate feedback from various users and create a prototype with a cleaner, simpler API. The base classes are the foundation on top of which higher level layers and tools can be built. For example, one could write a domain specific language to create the application graph. Look at this as the foundation and not as the end product.

Here is a list of ideas:

- Eliminate string identifiers completely and make it work nicely with Guice. 
- Take advantage of Java strong typing using generics in the framework. (But keep it simple for the app developer.)
- Use dependency injection, no more "new".
- Limit property files to simple parameters. Config file should not be
a programming language. (No more XML.)
- Configure the app in a GuiceModule.
- Make fields final private whenever possible and use constructors
instead of setter methods.
- Make PEs as immutable as possible.
- Hide details about partitioning from app developer. The idea is to
create a graph for the prototypes and have the base classes deal with
the distributed processing details.
- Use osgi to create s4 app bundles that can be dropped in a directory
and get loaded by s4. (This will take a bit longer to do.)

Please post feedback and suggestions in the s4 forum. 

The repo is here:

https://github.com/leoneu/s4-piper

In this version:

- Everything is a POJO.
- Key framework classes (io.s4) are:
  - Event: all messages sent between PEs inherit from Event.
  - ProcessingElement: base class for PEs.
  - Stream: takes an Event subclass as the parameter type for the stream.  A stream holds references to the target PEs. Streams are the edges between PEs.
  - Key: is a helper class that can return the value of a key for a specific type of event. For each type of event, one will create a KeyFinder class that knows how to get the value from the Event class.
  - App: the container for the app.

Note that when we create a PE in App, we are really creating a prototype of the PE. The actual PE instances get created every time a node receives an event that is dispatched to a specific PE prototype. The first PE has Map that holds the instances by a specific key. PE instances are clones which means that we don't call the constructor. Instead we provide an init method. Also, the instance variables are cloned. This means that references are also copied. If you wanted to have a List per PE instance, you would have to create it in the init method. This design pattern, makes thing very simple and efficient but has the downside that programmers have to understand how instances are created via clone.

For now we identify PEs using a composite string produced by KeyFinder. We must also pass the raw data types used to create the composite key. Need to think how to do this with type safety in mind.
 
To test the API I created a simple example (io.s4.example). It does the following:

- Generate dummy events (UserEvent).
- Key events by user, gender, age.
- Count by user, gender, age.
- Print partial counts.

In following diagram I show how Classes, PE Prototypes, PE instances, and Streams are related.

![S4 Objects](https://github.com/leoneu/s4-piper/raw/master/etc/s4-objects-example.png)


