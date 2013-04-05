<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->

Simple S4 Benchmarking Framework
================================

This framework is intended primarily to identify hotspots in S4 platform code easily and to evaluate the impact of refactorings or new features. 

> The numbers it produces are mainly useful in comparison with a baseline from other measurements from the same benchmark and do not represent absolute performance numbers. For that, one should use a full-fledged load injection framework or measure the performance of a live application.

That said, let's look at what the benchmarking framework does and how to use it.

## Measurements

The benchmarking framework consists of a multithreaded injector and an application. App nodes and injector are launched directly, there is no deployment step. This allows to skip the packaging and deployment steps and to easily add profiling parameters, but requires a source distribution and a shared file system.

2 simple applications are provided:

* A producer consumer application between 2 different logical clusters. It measures the overhead of the communication layer. There are 2 available external input streams: `inputStream` and `inputStream2`. You may inject on one or both of these independent streams. (A node will process more events overall if it gets them from more parallel sources, unless it reaches network or cpu boundaries). 
* A pipeline of processing elements, that mixes external and internal event communication

There is almost no processing involved in the PE themselves, other than delegating to the next processing element in the pipeline, if any.

The injector sends basic keyed messages to a given named stream. The outputstream of the injector uses a keyfinder to partition the events across the application nodes.

We get metrics from the probes across the codebase, in particular:
- the rate of events sent per second (in the injector)
- the rate of events received per second (in the app nodes)
- other metrics about the number of dequeue messages per stream, ratio between local and remote events etc...

Metrics from the platform code are computed with weighted moving averages. It is recommended to let the application run for a few minutes and observe the metrics from the last minute.

Profiling options (e.g. YourKit) can easily be added to the injector or app nodes in order to identify hotspots.

## Parameters

We provide a script for that purpose: `bench-cluster.sh`.


We can use arbitrary numbers of injectors and processing nodes, in order to vary the load and the number of concurrent connections.


Input parameters are:

- host names on which to start S4 nodes
	* they must either be : localhost, or accessible through ssh (_a shared file system is assumed_) 
- injector configuration (see below)
- node configuration (you __must__ specify the correct zookeeper connection string. By default, a server is created on the node where the `bench-cluster.sh` script is executed)

 
Example configuration files `config/injector.config` and `config/node.config` are not included in the source distribution but can be retrieved from the S4 git repository, in the `<root>/subprojects/s4-benchmarks/config` directory.

You can configure :

- the number of keys
- the number of test iterations
- the number of parallel injection threads
- the number of threads for the sender stage
- the number of events between making a pause in the injection
- the duration of the pause (can be 0)
- etc…
- It is also possible to limit the injection rate by using and configuring the InjectionLimiterModule class. Parameters are `s4.sender.maxRate` and `s4.sender.warmupPeriod`.


The total number of events sent from an injector is `number of keys * number of test iterations * number of parallel injection threads`. Make sure this is significant in order to be able to correctly interpret the messaging rates (1000 would be too little for instance!).

By default in this example the size of a message is 188 bytes.



## Running

Running 2 S4 nodes on the local machine:
`./bench-cluster.sh "localhost localhost" `pwd`/config/injector.config `pwd`/config/node.config`

For a distributed setup, you should modify the host names in the above command line, and specify the correct Zookeeper connection string in `node.config`.

Here is an example for driving the test on a cluster:
`incubator-s4/subprojects/s4-benchmarks/bench-cluster.sh "processingHost1 processingHost2 processingHost3" "injectorConfigStream1.cfg injectorConfigStream2.cfg" node.cfg 2 "injectorHost1 injectorHost2 injectorHost3 injectorHost4"` (the `2` controls the number of injectors per stream per injector host)


## Results


When the benchmark finishes (and even during the execution), results are available in `measurements/injectors` for the injection rates and in `measurements/node[0-n]` for other statistics.

Results are also available from the console output for each of the nodes.

Most statistics files come from the probes of the platform and some of them use weighted moving averages. These are good for long running applications. For the benchmarks we also show instant rates, which are available in `injection-rate.csv` and `simplePE1.csv` files.

You may also check that all events have been processed: 

* each injector reports how many events it sent on which stream
* each node reports the total number of events received
* depending on the injection rate, you can see how many events have been dropped, if any: `total injected from all injectors >= total received in all nodes` (minus events sent through internal streams in the app, if that applies)


## Notes

There are a lot of knobs for optimally configuring the stages, and the optimal settings will also depend upon:
- the hardware
- the network
- the operating system (scheduling, networking)
- the JVM implementation and tuning parameters
- the application
- the skewness and diversity of the data (there a maximum of 1 event executing in a given PE instance (i.e. keyed))