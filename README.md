S4-Piper Design
===============

- Eliminate string identifiers completely and make it work nicely with Guice.
- Use dependency injection, no more "new".
- Limit property files to simple parameters. Config file should not be
a programming language.
- Configure the app in a GuiceModule.
- Make fields final private whenever possible and use constructors
instead of setter methods.
- Make PEs as immutable as possible.
- Hide details about partitioning from app developer. The idea is to
create a graph for the prototypes and have the base classes deal with
the distributed processing details.
- Use osgi to create s4 app bundles that can be dropped in a directory
and get loaded by s4.

I think this can be pretty simple and clean. This is just a first
pass, we need to brainstorm and have a few iterations.

https://github.com/leoneu/s4-piper


- no prototype if possible - all pe instances the same.
- do we need Stream?
- how do we configure keys

The event/key is a atomic unit

A PE can listen to many event/keys

for a given PE inst. the set of key values of incoming events have to be the same. (pe inst is keyed on the value)

For each pe inst. each event type has to be mapped to a processing method

currently maps to method signature (which takes event as param) we are happy with this approach.

