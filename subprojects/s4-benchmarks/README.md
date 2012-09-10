Simple S4 Benchmarking Framework
================================

This framework is intended primarily to identify hotspots in S4 platform code easily and to evaluate the impact of refactorings or new features. 

> The numbers it produces are only useful in comparison with a baseline from other measurements from the same benchmark and do not represent absolute performance numbers. For that, one should use a full-fledged load injection framework or measure the performance of a live application.

That said, let's look at what the benchmarking framework does and how to use it.

## Measurements

The benchmarking framework consists of a multithreaded injector and an application. App nodes and injector are launched directly, there is no deployment step. This allows to skip the packaging and deployment steps and allows to easily add profiling parameters, but requires a source distribution and a shared file system.

The simplest application does nothing but count incoming keyed messages, on a single stream, but other simple application can be easily added. For instance, with multiple streams, and communicating PEs.

The injector sends basic keyed messages. The outputstream of the injector uses a keyfinder to partition the events across the application nodes.

We get metrics from the probes across the codebase, in particular:
- the rate of events sent per second (in the injector)
- the rate of events received per second (in the app nodes)

Metrics from the platform code are computed with weighted moving averages.

Profiling options can easily be added to the injector or app nodes in order to identify hotspots.

## Parameters

We provide a script for that purpose: `bench-cluster.sh`

Input parameters are:

- host names on which to start S4 nodes
	* they must either be : localhost, or accessible through ssh (_a shared file system is assumed_) 
- injector configuration (see below)
- node configuration (you __must__ specify the correct zookeeper connection string. By default, a server is created on the node where the `bench-cluster.sh` script is executed)

 
Exmample configuration files are available in `/config` and you can configure :

- the number of keys
- the number of warmup iterations
- the number of test iterations
- the number of parallel injection threads
- etc…

By default in this example the size of a message is 188 bytes.


## Running

Running 2 S4 nodes on the local machine:
`./bench-cluster.sh "localhost localhost" `pwd`/config/injector.config `pwd`/config/node.config`

For a distributed setup, you should modify the host names in the above command line, and specify the correct Zookeeper connection string in `node.config`.

## Results


When the benchmark finishes, results are available in `measurements/injectors` for the injection rates and in `measurements/node[0-n]` for other statistics.

Most statistics files come from the probes of the platform and some of them use weighted moving averages. These are good for long running applications. For the benchmarks we also show instant rates, which are available in `injection-rate.csv` and `simplePE1.csv` files.

