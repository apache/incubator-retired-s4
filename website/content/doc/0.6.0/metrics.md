---
title: Metrics
---


> S4 continuously collects runtime statistics. Let's see how to access these and add custom ones.

# Why?

S4 aims at processing large quantities of events with low latency. In order to achieve this goal, a key requirement is to be able to monitor system internals at runtime.

# How?
For that purpose, we include a system for gathering statistics about various parts of the S4 platform.

We rely on the [metrics](http://metrics.codahale.com) library, which offers an efficient way to gather such information and relies on statistical techniques to minimize memory consumption.

# What?

By default, S4 instruments queues, caches, checkpointing, event reception and emission and statistics are available for all of these components.

You can also monitor your own PEs. Simply add new probes (`Meter`, `Gauge`, etc..) and report interesting updates to them. There is nothing else to do, these custom metrics will be reported along with the S4 metrics, as explained next.

# Where? 

By default, metrics are exposed by each node through JMX.

The `s4.metrics.config` parameter enables periodic dumps of aggregated statistics to the **console** or to **files** in csv format. This parameter is specified as an application parameter, and must match the following regular expression: 

	(csv:.+|console):(\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)

Examples:
	
	# dump metrics to csv files to /path/to/directory every 10 seconds
	# (recommendation: use a clean directory)
	csv:/path/to/directory:10:SECONDS
	
	# dump metrics to the console every minute
	console:1:MINUTES
	
	

Reporting to Ganglia or Graphite is not provided out of the box with S4, but it's quite easy to add. You simply have to add the corresponding dependencies to your project and enable reporting to these systems during the initialization of your application. See the [metrics](http://metrics.codahale.com) documentation for more information.