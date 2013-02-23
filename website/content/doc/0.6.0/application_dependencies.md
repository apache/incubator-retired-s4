---
title: Adding application dependencies
---

> Make sure you have already read the [walkthrough](../walkthrough)

# How to add dependencies to my S4 application?

Your application typically depends on various external libraries. Here is how to configure those dependencies in an S4 project. We assume here that you are working with a sample project automatically generated through the `s4 newApp` script.

## Dependencies on public artifacts

* Add maven artifacts definitions to the gradle build file. For instance, add twitter4j_core and twitter4j_stream:


		project.ext["libraries"] = [
		           twitter4j_core:     'org.twitter4j:twitter4j-core:2.2.5',
		           twitter4j_stream:   'org.twitter4j:twitter4j-stream:2.2.5',
		           s4_base:            'org.apache.s4:s4-base:0.5.0',
		           s4_comm:            'org.apache.s4:s4-comm:0.5.0',
		           s4_core:            'org.apache.s4:s4-core:0.5.0'
		       ]

* Add these dependencies as compile-time dependencies. For instance:

		dependencies {
		   compile (libraries.s4_base)
		   compile (libraries.s4_comm)
		   compile (libraries.s4_core)
		   compile (libraries.twitter4j_core)
		   compile (libraries.twitter4j_stream)
		}

* If you use an IDE such as eclipse, you may update your project's classpath with: `./gradlew eclipse`

A good source for finding dependencies is for instance [http://search.maven.org/](http://search.maven.org/) where you also get the syntax for gradle scripts (see grails syntax).


>The application dependencies will be automatically included in the s4r archive that you create and publish.


## Dependencies on non-public artifacts

You may have dependencies that are not published to maven repositories. In that case you should either:

* publish them to your local maven repository, see [http://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html]([http://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html)
* add them to the _lib_ directory

In both cases you still have to declare them as compile-time dependencies.