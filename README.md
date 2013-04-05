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

Apache S4
=========

Overview
--------
S4 is a general-purpose,near real-time, distributed, decentralized, scalable, 
event-driven, modular platform that allows programmers to easily implement applications 
for processing continuous unbounded streams of data.

S4 0.5 focused on providing a functional complete refactoring.

S4 0.6 builds on this basis and brings several major improvements, in particular:

- major performance improvements: stream throughput improved (measured up to ~200k messages per second and per stream)
- major configurability improvements, for both the S4 platform and deployed applications


Documentation
-------------

For the latest information about S4, please visit our website at:

	   http://incubator.apache.org/s4

Source code is available here: https://git-wip-us.apache.org/repos/asf?p=incubator-s4.git


Requirements
------------
* JDK 6 or higher
* *nix or macosx (you may build the project and develop S4 applications with 
microsoft windows though, the only limitation is that the "s4" script has not 
been ported to windows yet)


How to build
------------
This only applies if you checkout from the source repository or if you download a 
released source package.


We use gradle `http://gradle.org` as the build system, and we use the gradle wrapper script (`gradlew`) in some of the S4 tools.


### Installing the gradle wrapper

> Instructions apply to source release downloads. When checking out from the git repository, the gradle wrapper is already available.


Gradle is not shipped with the source distribution of S4, so you'll need to :

1. Install gradle: follow the instructions in the [gradle web site](http://gradle.org). We tested S4 with gradle 1.4.
2. Generate the gradle wrapper: from the root directory of the S4 project, execute: `/path/to/gradle wrapper`
	* this will generate `gradlew` and `gradlew.bat` script and place `gradle-wrapper-1.4.jar` and `gradle-wrapper-1.4.properties` in the `lib` directory

### Building the project 

* From the root directory of the S4 project:

		./gradlew install

This will build the packages and install the artifacts in the local maven repository.

* Then, build the tools:

		./gradlew s4-tools:installApp

This will build the tools so that you can drive the platform through the "s4" command.

* You may also run regression tests, after artifacts are installed:

		./gradlew test


Directory structure
-------------------

* If you checked out from the git repository:

	- root directory contains build and utility scripts, as well as informative files
	- config/ directory contains configuration files for source code formatting
	- lib/ directory contains libraries for building the project and validating source headers
	- subprojects/ directory contains the plaftorm subprojects: base, comm, core, tools, 
	as well as example (example is not fully ported to 0.5.0+)
	- test-apps/ directory contains some examples (some of them very trivial are used 
	in regression tests)
	- website/ directory contains the source of the website, including documentation



* If you have a source package:

	- root directory contains build and utility scripts, as well as informative files
	- doc/ directory contains the javadoc
	- subproject/ directory contains the plaftorm subprojects: base, comm, core, tools, 
	as well as example (example is not fully ported to 0.5.0)
	- test-apps/ directory contains some examples (some of them very trivial are used 
	in regression tests)



* If you have a binary package:

	- root directory contains the s4 utility script as well as informative files
	- doc/ directory contains the javadoc
	- lib/ directory contains :
		* the platform libraries (prefixed with "s4")
		* the dependencies
	- bin/ directory contains some scripts that are used by the s4 script



