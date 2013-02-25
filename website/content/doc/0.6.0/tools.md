---
title: S4 commands
---


> List of commands

S4 ships with a toolkit for creating, packaging, deploying and running applications.

From the source distribution, these tools are built by running:

	gradlew s4-tools:installApp

This compiles the s4-tools subproject and generates shell scripts.


# Available commands

Here is the list of commands available with the `s4` tool. For each of these commands, the comprehensive documentation of all parameters is shown by specifying the `-help` option.

Syntax: `s4 <command> <options>`

|---
| Purpose | Description | Command 
|-|-|-
| Create a new application | Create a bootstrap project skeleton | `newApp`
| Start a ZooKeeper server instance | Useful for testing | `zkServer`
| Define an S4 cluster | Specify cluster size and initial ports for listening sockets | `newCluster`
| Package an application | S4R archive to be deployed on S4 nodes |  `s4r` |
| Deploy/configure an application | Specifies application and platform configuration | `deploy`
| Start an S4 node | S4 node bootstrap process, connects to the cluster manager and fetches app and platform configuration, as specified through `deploy` command | `node`
| Get information about S4 infrastructure | Shows status of S4 clusters, apps, nodes and external streams | `status`
|---


In addition, for easy injection of data, the `adapter` command allows to start an node without having to package and deploy the application.


# Undeploying an application

There is currently no specific command for undeploying S4 applications. The recomended way for removing an application deployed on cluster C1 is to:

* kill S4 nodes belonging to cluster C1
* delete ZooKeeper subtree /s4/clusters/C1
* redefine cluster C1
* deploy new application
* restart nodes for cluster C1 (this could be automated with some utility like [daemontools](http://cr.yp.to/daemontools.html))