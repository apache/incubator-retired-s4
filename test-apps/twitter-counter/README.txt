#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

An application that displays the current top 10 topics, as gathered from the twitter sample stream.
It was ported and adapted from S4 0.3

Architecture:
- twitter-adapter app in adapter node connects to the twitter stream, extracts the tweeted text and passes that to the application cluster
- twitter-counter app in the application cluster receives the text of the tweets, extracts the topics, counts topic occurences and periodically displays the top 10 topics on the console

How to configure:
- you need a twitter4j.properties file in your home dir, with the following properties filled:
debug=true|false
user=<a twitter user name>
password=<the matching password>

How to run:

Please follow the instructions in the S4 piper walkthrough on the documentation from the website