---
title: S4 0.6.0 overview
---



# What is S4?

S4 is a general-purpose,near real-time, distributed, decentralized, scalable, event-driven, modular platform that allows programmers to easily implement applications for processing continuous unbounded streams of data.


S4 0.5 focused on providing a functional complete refactoring.

S4 0.6 builds on this basis and brings plenty of exciting features, in particular:

* *performance improvements*: stream throughput improved by 1000 % (~200k messages / s / stream)
* improved [configurability](S4:Configuration - 0.6.0], for both the S4 platform and deployed applications
* *elasticity* and fine partition tuning, through an integration with Apache Helix


# What are the cool features?

**Flexible deployment**:

* By default keys are homogeneously sparsed over the cluster: helps balance the load, especially for fine grained partitioning
* S4 also provides fine control over the partitioning
* Features automatic rebalancing

**Modular design**:

* both the platform and the applications are built by dependency injection, and configured through independent modules.
* makes it easy to customize the system according to specific requirements
* pluggable event serving policies: load shedding, throttling, blocking

**Dynamic and loose coupling of S4 applications**:

* through a pub-sub mechanism
* makes it easy to:
** assemble subsystems into larger systems
** reuse applications
** separate pre-processing
** provision, control and update subsystems independently


**[Fault tolerant](fault_tolerance)**


* *Fail-over* mechanism for high availability
* *Checkpointing and recovery* mechanism for minimizing state loss

**Pure Java**: statically typed, easy to understand, to refactor, and to extend




# How does it work?

## Some definitions

**Platform**

* S4 provides a runtime distributed platform that handles communication, scheduling and distribution across containers.
* Distributed containers are called *S4 nodes*
* S4 nodes are deployed on *S4 clusters*
* S4 clusters define named ensembles of S4 nodes, with a fixed size
* The size of an S4 cluster corresponds to the number of logical *partitions* (sometimes referred to as _tasks_)

**Applications**



* Users develop applications and deploy them on S4 clusters
* Applications are built from:
** *Processing elements* (PEs)
** *Streams* that interconnect PEs

* PEs communicate asynchronously by sending *events* on streams.
* Events are dispatched to nodes according to their key

**External streams** are a special kind of stream that:



* send events outside of the application
* receive events from external sources
* can interconnect and assemble applications into larger systems.

**Adapters** are S4 applications that can convert external streams into streams of S4 events. Since adapters are also S4 applications, they can be scaled easily.




## A hierarchical perspective on S4

The following diagram sums-up the key concepts in a hierarchical fashion:

![image](/images/doc/0.6.0/S4_hierarchical_archi.png)

# Where can I find more information?

* [The website](http://incubator.apache.org/s4/) is a good starting point.
* [The wiki](https://cwiki.apache.org/confluence/display/S4/) currently contains the most up-to-date information: general information (this page), configuration, examples.
* Questions can be asked through the [mailing lists](https://cwiki.apache.org/confluence/display/S4/S4+Apache+mailing+lists)
* The source code is available throught [git](https://git-wip-us.apache.org/repos/asf?p=incubator-s4.git], [here](http://incubator.apache.org/s4/contrib/) are instructions for fetching the code.
* A nice set of [slides](http://www.slideshare.net/leoneu/20111104-s4-overview) was used for a presentation at Stanford in November 2011.
* The driving ideas are detailed in a [conference publication](http://www.4lunas.org/pub/2010-s4.pdf) from KDCloud'11 (joint workshop with ICDM'11)