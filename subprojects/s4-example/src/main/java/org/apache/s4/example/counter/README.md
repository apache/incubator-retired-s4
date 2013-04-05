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

S4 Counter Example (NOT updated for 0.5.0)
==================

In this example we do the following:

- Generate dummy events (UserEvent).
- Key events by user, gender, age.
- Count by user, gender, age.
- Print partial counts.

The following diagram shows the application graph:

![S4 Counter](https://github.com/leoneu/s4-piper/raw/master/etc/s4-counter-example.png)

In in following diagram I show how Classes, PE Prototypes, PE instances, and Streams are related.

![S4 Objects](https://github.com/leoneu/s4-piper/raw/master/etc/s4-objects-example.png)
