---
title: Configuration
---

# Toolset

S4 provides a set of tools to:

* define S4 clusters: `s4 newCluster`
* start S4 nodes: `s4 node`
* package applications: `s4 s4r`
* deploy applications: `s4 deploy`
* start a Zookeeper server for easy testing: `s4 zkServer`
	* `s4 zkServer -t` will start a Zookeeper server and automatically configure 2 clusters
* view the status of S4 clusters coordinated by a given Zookeeper ensemble: `s4 status`


		./s4

will  give you a list of available commands.

	./s4 <command> -help

will provide detailed documentation for each of these commands.


# Cluster configuration

Before starting S4 nodes, you must define a logical cluster by specifying:

* a name for the cluster
* a number of partitions (~ tasks)
* an initial port number for listener sockets
	* you must specify a free port, considering that each of the nodes of the cluster will open a different port, with a number monotonically increasing from the initial number. For instance, for a cluster of 10 nodes and an initial port 12000, ports 12000 to 12009 will be used among the nodes.
	* those ports are used for inter node communication.


The cluster configuration is maintained in Zookeeper, and can be set using S4 tools:

	./s4 newCluster -c=cluster1 -nbTasks=2 -flp=12000
See tool documentation by typing:
	
	./s4 newCluster -help


# Node configuration

*Platform* *code and* *application* *code are fully configurable,* *at deployment time{*}*.*

S4 nodes start as simple *bootstrap* processes whose initial role is merely to connect the cluster manager:

* the bootstrap code connects to the cluster manager
* when an application is available on the cluster, the node gets notified
* it downloads the platform configuration and code, as specified in the configuration of the deployed application.
* the communication and core components are loaded, bound and initialized
* the application configuration and code, as specified in the configuration of the deployed applciation, is downloaded
* the application is initialized and started

This figure illustrates the separation between the bootstrap code, the S4 platform code, and application code in an S4 node:


![image](/images/doc/0.6.0/s4_node_layers.png)


Therefore, for starting an S4 node on a given host, you only need to specify:

* the connection string to the cluster management system (Zookeeper) `localhost:2181` by default
* the name of the logical cluster to which this node will belong

Example:
`./s4 node -c=cluster1 -zk=host.domain.com`


# Application configuration

Deploying applications is easier when we can define both the parameters of the application *and* the target environment.

In S4, we achieve this by specifying *both* application parameters and S4 platform parameters in the deployment phase :

* which application class to use
* where to fetch application code
* which specific modules to use
* where to fetch these modules
* string configuration parameters - that can be used by the application and the modules



## Modules configuration

S4 follows a modular design and uses[Guice](http://code.google.com/p/google-guice/) for defining modules and injecting dependencies.

As illustrated above, an S4 node is composed of:
* a base module that specifies how to connect to the cluster manager and how to download code
* a communication module that specifies communication protocols, event listeners and senders
* a core module that specifies the deployment mechanism, serialization mechanism
* an application

### default parameters

For the [comm module](https://github.com/apache/incubator-s4/blob/dev/subprojects/s4-comm/src/main/resources/default.s4.comm.properties): communication protocols, tuning parameters for sending events

For the core module, there is no default parameters.

### overriding modules

We provide default modules, but you may directly specify others through the command line, and it is also possible to override them with new modules and even specify new ones (custom modules classes must provide an empty no-args constructor).

Custom overriding modules can be specified when deploying the application, through the`deploy` command, through the _emc_ or _modulesClasses_ option.

For instance, in order to enable file system based checkpointing, pass the corresponding checkpointing module class :

	./s4 deploy -s4r=uri/to/app.s4r -c=cluster1 -appName=myApp \
	-emc=org.apache.s4.core.ft.FileSystemBackendCheckpointingModule 

You can also write your own custom modules. In that case, just package them into a jar file, and specify how to fetch that file when deploying the application, with the _mu_ or _modulesURIs_  option.

For instance, if you checkpoint through a specific key value store, you can write you own checkpointing implementation and module, package that into fancyKeyValueStoreCheckpointingModule.jar , and then:

	./s4 node -c=cluster1 -emc=my.project.FancyKeyValueStoreBackendCheckpointingModule \
	-mu=uri/to/fancyKeyValueStoreCheckpointingModule.jar

### overriding parameters

A simple way to pass parameters to your application code is by:

* injecting them in the application class:

		@Inject
		@Named('myParam')
		param
* specifying the parameter value at node startup (using -p inline with the node command, or with the '@' syntax)

S4 uses an internal Guice module that automatically injects configuration parameters passed through the deploy command to matching `@Named` parameters.

Both application and platform parameters can be overriden. For instance, specifying a custom storage path for the file system based checkpointing mechanism would be passing the `s4.checkpointing.filesystem.storageRootPath` parameter:

	./s4 deploy -s4r=uri/to/app.s4r -c=cluster1 -appName=myApp \
	-emc=org.apache.s4.core.ft.FileSystemBackendCheckpointingModule \ 
	-p=s4.checkpointing.filesystem.storageRootPath=/custom/path 

## File-based configuration

Instead of specifying node parameters inline, you may refer to a file with the '@' notation:
./s4 deploy @/path/to/config/file
With contents of the referenced file like:

	-s4r=uri/to/app.s4r
	-c=cluster1
	-appName=myApp
	-emc=org.apache.s4.core.ft.FileSystemBackendCheckpointingModule
	-p=param1=value1,param2=value2




## Logging

S4 uses [logback](http://logback.qos.ch/), and [here](https://git-wip-us.apache.org/repos/asf?p=incubator-s4.git;a=blob_plain;f=subprojects/s4-core/src/main/resources/logback.xml;h=ea8c85a104b475f1b9dea641656e76eb3b6a9d4c;hb=piper) is the default configuration file. You may tweak this configuration by adding your own logback.xml file in the `lib/` directory (for a binary release) or in the `subprojects/s4-tools/build/install/s4-tools/lib/` directory (for a source release or checkout from git).